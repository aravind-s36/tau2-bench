"""Discrete-time adapter for the atoms cascade, run locally as one bot per call.

Each call spawns ``atoms.evals.tau_bridge.atoms_tau_bot`` from a pipecat-v2
checkout and talks to it as a Twilio media stream over a local WebSocket: tau2's
8 kHz mu-law caller audio goes out as ``media`` frames, the bot's audio comes
back the same way, and ``clear`` marks a barge-in. The bot also sends each tool
call (``atoms_tool_call``, answered with ``atoms_tool_result``) and each word as
its transport plays it (``atoms_word``), ending each TTS context with
``atoms_speech_end``.

The run is configured through the environment, which ``atoms/evals/tau_bridge/run.py``
sets:

- ``ATOMS_TAU_REPO``: the pipecat-v2 checkout whose ``.venv`` runs the bot.
- ``ATOMS_TAU_OUTPUT``: the folder each call's bot config, log and traces go to.
- ``ATOMS_TAU_BOT``: JSON of the bot settings every call shares (model,
  agent-config overrides, prompt-harness edits).
- ``ATOMS_TAU_ENV_FILE``: the keys file the bot loads.
- ``ATOMS_TAU_START_SPACING_S``: the least time between two bots starting.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import socket
import subprocess
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any

import aiohttp
import websockets
from loguru import logger

from tau2.config import (
    DEFAULT_AUDIO_NATIVE_DISCONNECT_TIMEOUT,
    DEFAULT_AUDIO_NATIVE_TICK_TIMEOUT_BUFFER,
)
from tau2.data_model.audio import TELEPHONY_AUDIO_FORMAT, AudioFormat
from tau2.data_model.message import ToolCall
from tau2.environment.tool import Tool
from tau2.voice.audio_native.adapter import DiscreteTimeAdapter
from tau2.voice.audio_native.async_loop import BackgroundAsyncLoop
from tau2.voice.audio_native.tick_result import TickResult

DEFAULT_ATOMS_MODEL = "electron-2.0"
_BOT_READY_TIMEOUT_S = 90.0
_BOT_EXIT_TIMEOUT_S = 10.0
_VOIP_CHUNK_BYTES = 160  # 20 ms of 8 kHz mu-law
# "The current time is 2024-05-15 15:00:00 EST." in the airline policy.
_POLICY_CLOCK = re.compile(
    r"current time is (\d{4}-\d{2}-\d{2} \d{2}:\d{2})(?::\d{2})? ([A-Z]{2,4})"
)

# Bots start one at a time, ATOMS_TAU_START_SPACING_S apart, across tau2's
# worker threads: each one opens its LLM, STT and TTS sessions at once.
_start_lock = threading.Lock()
_last_start = [0.0]


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def policy_clock(system_prompt: str) -> str | None:
    """The scenario's "now" as ``YYYY-MM-DD HH:MM ZONE``, if the policy states one."""
    match = _POLICY_CLOCK.search(system_prompt)
    return f"{match.group(1)} {match.group(2)}" if match else None


class PlayedWords:
    """One utterance's text, released only as far as its audio has played.

    The bot sends each word as its transport starts to play it, so a word
    arrives just ahead of its own audio. It is marked at the audio received so
    far, and released once tau2 has played that much of the utterance.

    Implements the ``get_transcript_for_audio`` protocol that
    ``get_proportional_transcript`` calls on the audio played each tick.
    """

    def __init__(self) -> None:
        """An utterance with no audio or words yet."""
        self.text = ""
        self._received = 0
        self._played = 0
        self._shown = 0
        self._marks: deque[tuple[int, int]] = deque()

    def add_audio(self, num_bytes: int) -> None:
        """Count ``num_bytes`` more of the utterance's audio as received."""
        self._received += num_bytes

    def add_word(self, word: str) -> None:
        """Append a word, to be released once the audio received so far has played."""
        word = word.strip()
        if not word:
            return
        self.text = f"{self.text} {word}" if self.text else word
        self._marks.append((self._received, len(self.text)))

    def interrupt(self) -> None:
        """Forget the words the caller has not heard."""
        self.text = self.text[: self._shown]
        self._marks.clear()

    def get_transcript_for_audio(self, audio_bytes_to_play: int) -> str:
        """The words released by playing ``audio_bytes_to_play`` more bytes."""
        self._played += audio_bytes_to_play
        end = self._shown
        while self._marks and self._marks[0][0] <= self._played:
            end = self._marks.popleft()[1]
        # The space before a tick's first word stays: tau2 joins the ticks' text
        # with no separator, so stripping it runs words together ("Andyour").
        text = self.text[self._shown : end]
        self._shown = max(self._shown, end)
        return text if text.strip() else ""


class DiscreteTimeAtomsAdapter(DiscreteTimeAdapter):
    """Bridge tau2 telephony ticks to a local atoms cascade bot."""

    def __init__(
        self,
        tick_duration_ms: int,
        send_audio_instant: bool = False,
        model: str | None = None,
        reasoning_effort: str | None = None,
        audio_format: AudioFormat | None = None,
    ):
        """Read the run's settings from the environment ``run.py`` sets."""
        if reasoning_effort is not None:
            raise ValueError(
                "atoms reasoning is set by its model; --reasoning-effort is not supported"
            )
        if audio_format is not None and audio_format != TELEPHONY_AUDIO_FORMAT:
            raise ValueError(
                f"atoms speaks telephony audio (8 kHz mu-law) only, got {audio_format}"
            )
        super().__init__(
            tick_duration_ms,
            audio_format=audio_format,
            send_audio_instant=send_audio_instant,
        )
        self.model = model or DEFAULT_ATOMS_MODEL
        self.reasoning_effort = None
        # The bot speaks and listens on the wall clock, as a phone call does.
        self.realtime_pacing = True

        self._repo = Path(os.environ["ATOMS_TAU_REPO"])
        self._output = Path(os.environ["ATOMS_TAU_OUTPUT"])
        self._bot_settings = json.loads(os.environ.get("ATOMS_TAU_BOT") or "{}")
        self._env_file = os.environ.get("ATOMS_TAU_ENV_FILE")
        self._start_spacing = float(os.environ.get("ATOMS_TAU_START_SPACING_S") or 3.0)

        self._bg_loop = BackgroundAsyncLoop()
        self._connected = False
        self._process: subprocess.Popen | None = None
        self._log_file = None
        self._ws: Any = None
        self._reader: asyncio.Task | None = None
        self._events: asyncio.Queue | None = None
        self.call_dir: Path | None = None
        self._end_hold_bytes = self.audio_format.bytes_per_second // 1000
        self._reset_speech_state()

    def _reset_speech_state(self) -> None:
        # The bot's audio bytes received, for the playback reports (atoms_played).
        self._agent_bytes_in = 0
        self._played_reported = -1
        self._utterance_count = 0
        # The utterance new audio and words belong to, open from its first
        # audio or word until its speech end or a barge-in.
        self._utterance_id: str | None = None
        # The last millisecond of an open utterance, held back so its last
        # words, released on that audio, never land on a tick without audio
        # (the user simulator drops text from silent ticks).
        self._held_end: list[tuple[bytes, str | None]] = []

    @property
    def is_connected(self) -> bool:
        """Whether the bot's media stream is open."""
        return self._connected and self._bg_loop.is_running

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(
        self,
        system_prompt: str,
        tools: list[Tool],
        vad_config: Any = None,
        modality: str = "audio",
    ) -> None:
        """Start this call's bot and open its media stream."""
        if modality != "audio":
            raise ValueError("atoms supports audio modality only")
        if self._connected:
            self.disconnect()
        self._bg_loop.start()
        try:
            self._bg_loop.run_coroutine(
                self._async_connect(system_prompt, tools),
                timeout=_BOT_READY_TIMEOUT_S + 30 * self._start_spacing + 10,
            )
        except Exception as exc:
            self._stop_bot()
            self._bg_loop.stop()
            raise RuntimeError(f"Failed to start the atoms bot: {exc}") from exc
        self._connected = True
        logger.info(f"DiscreteTimeAtomsAdapter connected (call dir {self.call_dir})")

    async def _async_connect(self, system_prompt: str, tools: list[Tool]) -> None:
        call_id = f"tau-{uuid.uuid4().hex[:12]}"
        self.call_dir = self._output / call_id
        self.call_dir.mkdir(parents=True, exist_ok=True)
        config_path = self.call_dir / "atoms_bot_config.json"
        config_path.write_text(
            json.dumps(
                {
                    **self._bot_settings,
                    "model": self.model,
                    "call_id": call_id,
                    "system_prompt": system_prompt,
                    "tools": [tool.openai_schema for tool in tools],
                    "current_date_time": policy_clock(system_prompt),
                },
                ensure_ascii=False,
                indent=1,
            )
        )
        # tau2's per-task log names the call, to find its bot log and traces.
        logger.info(f"atoms call {call_id}: {self.call_dir}")

        while True:
            with _start_lock:
                wait = _last_start[0] + self._start_spacing - time.monotonic()
                if wait <= 0:
                    _last_start[0] = time.monotonic()
                    break
            await asyncio.sleep(wait)

        port = _free_port()
        await asyncio.to_thread(self._spawn_bot, config_path, port)

        deadline = time.monotonic() + _BOT_READY_TIMEOUT_S
        async with aiohttp.ClientSession() as session:
            while True:
                if self._process.poll() is not None:
                    raise RuntimeError(
                        f"bot exited with {self._process.returncode}; see {self.call_dir}/atoms_bot.log"
                    )
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"bot not ready after {_BOT_READY_TIMEOUT_S:.0f} s"
                    )
                try:
                    async with session.get(
                        f"http://127.0.0.1:{port}/health"
                    ) as response:
                        if response.status == 200:
                            break
                except aiohttp.ClientError as e:
                    logger.debug(f"atoms bot not serving yet: {e}")
                await asyncio.sleep(0.2)

        self._ws = await websockets.connect(f"ws://127.0.0.1:{port}/ws", max_size=None)
        await self._ws.send(
            json.dumps({"event": "connected", "protocol": "Call", "version": "1.0.0"})
        )
        await self._ws.send(
            json.dumps(
                {
                    "event": "start",
                    "streamSid": call_id,
                    "start": {
                        "streamSid": call_id,
                        "callSid": call_id,
                        "mediaFormat": {
                            "encoding": "audio/x-mulaw",
                            "sampleRate": 8000,
                            "channels": 1,
                        },
                    },
                }
            )
        )
        self._events = asyncio.Queue()
        self._reader = asyncio.create_task(self._read_events())

    def _spawn_bot(self, config_path: Path, port: int) -> None:
        self._log_file = open(self.call_dir / "atoms_bot.log", "w")
        env = dict(os.environ)
        env["PYTHONPATH"] = str(self._repo)
        command = [
            str(self._repo / ".venv" / "bin" / "python"),
            "-m",
            "atoms.evals.tau_bridge.atoms_tau_bot",
            "--config",
            str(config_path),
            "--port",
            str(port),
        ]
        if self._env_file:
            command += ["--env-file", self._env_file]
        self._process = subprocess.Popen(
            command,
            cwd=self._repo,
            env=env,
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
        )

    async def _read_events(self) -> None:
        try:
            async for raw in self._ws:
                await self._events.put(json.loads(raw))
        except websockets.ConnectionClosed as e:
            logger.debug(f"atoms bot media stream closed: {e}")
        finally:
            await self._events.put({"event": "atoms_closed"})

    def disconnect(self) -> None:
        """Hang up, and stop the bot."""
        if self._connected and self._bg_loop.is_running:
            try:
                self._bg_loop.run_coroutine(
                    self._async_disconnect(),
                    timeout=DEFAULT_AUDIO_NATIVE_DISCONNECT_TIMEOUT,
                )
            except Exception as exc:
                logger.warning(f"Error disconnecting the atoms bot: {exc}")
        self._stop_bot()
        self._bg_loop.stop()
        self._connected = False
        self._tick_count = 0
        self._cumulative_user_audio_ms = 0
        self.clear_buffers()
        self._reset_speech_state()
        logger.info("DiscreteTimeAtomsAdapter disconnected")

    async def _async_disconnect(self) -> None:
        if self._ws is None:
            return
        try:
            await self._ws.send(
                json.dumps({"event": "stop", "streamSid": self.call_dir.name})
            )
            await self._ws.close()
        finally:
            if self._reader is not None:
                self._reader.cancel()

    def _stop_bot(self) -> None:
        if self._process is not None and self._process.poll() is None:
            try:
                self._process.wait(_BOT_EXIT_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                self._process.terminate()
                try:
                    self._process.wait(_BOT_EXIT_TIMEOUT_S)
                except subprocess.TimeoutExpired:
                    self._process.kill()
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    # ------------------------------------------------------------------
    # Ticks
    # ------------------------------------------------------------------

    def run_tick(self, user_audio: bytes, tick_number: int | None = None) -> TickResult:
        """Exchange one tick of audio and events with the bot."""
        if not self.is_connected:
            raise RuntimeError("Not connected to the atoms bot. Call connect() first.")
        if tick_number is None:
            tick_number = self._tick_count
        self._tick_count = tick_number + 1
        return self._bg_loop.run_coroutine(
            self._async_run_tick(user_audio, tick_number),
            timeout=self.tick_duration_ms / 1000
            + DEFAULT_AUDIO_NATIVE_TICK_TIMEOUT_BUFFER,
        )

    async def _async_run_tick(self, user_audio: bytes, tick_number: int) -> TickResult:
        result = await super()._async_run_tick(user_audio, tick_number)
        self._buffered_agent_audio.extend(self._held_end)
        self._held_end = []
        await self._report_played()
        return result

    async def _report_played(self) -> None:
        """Tell the bot how much of its audio tau2 has used up: played, or dropped at a barge-in.

        The bot sends its audio at most a little ahead of this
        (``atoms/evals/tau_bridge/playback_sync.py``), so what it takes as
        playing is what tau2 plays, on tau2's ticks rather than the wall clock.
        """
        buffered = sum(len(audio) for audio, _ in self._buffered_agent_audio)
        played = self._agent_bytes_in - buffered
        if played == self._played_reported:
            return
        self._played_reported = played
        try:
            await self._ws.send(json.dumps({"event": "atoms_played", "bytes": played}))
        except websockets.ConnectionClosed:
            pass  # the bot is gone; the next event says so

    async def _flush_pending_tool_results(self) -> None:
        for call_id, content, _request_response, is_error in self._pending_tool_results:
            await self._ws.send(
                json.dumps(
                    {
                        "event": "atoms_tool_result",
                        "id": call_id,
                        "content": content,
                        "is_error": is_error,
                    }
                )
            )
        self._pending_tool_results.clear()

    async def _execute_tick(
        self,
        user_audio: bytes,
        tick_number: int,
        result: TickResult,
        tick_start: float,
    ) -> None:
        async def send_audio(chunk: bytes) -> None:
            await self._ws.send(
                json.dumps(
                    {
                        "event": "media",
                        "streamSid": self.call_dir.name,
                        "media": {"payload": base64.b64encode(chunk).decode("ascii")},
                    }
                )
            )

        async def receive_events() -> list[dict]:
            loop = asyncio.get_running_loop()
            deadline = tick_start + self.tick_duration_ms / 1000
            events = []
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    events.append(await asyncio.wait_for(self._events.get(), remaining))
                except TimeoutError:
                    break
            while not self._events.empty():
                events.append(self._events.get_nowait())
            return events

        sender = asyncio.create_task(
            self._send_audio_chunked(user_audio, send_audio, _VOIP_CHUNK_BYTES)
        )
        try:
            events = await receive_events()
            await sender
        finally:
            if not sender.done():
                sender.cancel()
        for event in events:
            self._process_event(result, event)
        self._hold_utterance_end(result)

    def _transcript(self) -> PlayedWords:
        if self._utterance_id is None:
            self._utterance_count += 1
            self._utterance_id = f"atoms-{self._utterance_count}"
            self._utterance_transcripts[self._utterance_id] = PlayedWords()  # type: ignore[assignment]
        return self._utterance_transcripts[self._utterance_id]  # type: ignore[return-value]

    def _hold_utterance_end(self, result: TickResult) -> None:
        if self._utterance_id is None or result.was_truncated:
            return
        available = sum(len(chunk) for chunk, _ in result.agent_audio_chunks)
        if available == 0 or available > self.bytes_per_tick:
            return  # nothing to play, or the tail is buffered anyway
        hold = min(available, self._end_hold_bytes)
        data, item_id = result.agent_audio_chunks.pop()
        while len(data) < hold:
            previous, item_id = result.agent_audio_chunks.pop()
            data = previous + data
        if len(data) > hold:
            result.agent_audio_chunks.append((data[:-hold], item_id))
        self._held_end = [(data[-hold:], item_id)]

    def _barge_in(self, result: TickResult) -> None:
        queued = sum(len(audio) for audio, _ in result.agent_audio_chunks)
        queued += sum(len(audio) for audio, _ in self._buffered_agent_audio)
        if queued == 0 and self._utterance_id is None:
            return  # nothing was playing
        result.was_truncated = True
        result.vad_events.append("interrupted")
        result.truncated_audio_bytes += queued
        result.agent_audio_chunks.clear()
        self._buffered_agent_audio.clear()
        self._held_end = []
        for transcript in self._utterance_transcripts.values():
            transcript.interrupt()  # type: ignore[attr-defined]
        self._utterance_id = None

    def _process_event(self, result: TickResult, event: dict) -> None:
        kind = event.get("event")
        if kind == "media":
            audio = base64.b64decode(event["media"]["payload"])
            if audio:
                self._agent_bytes_in += len(audio)
                transcript = self._transcript()
                transcript.add_audio(len(audio))
                result.agent_audio_chunks.append((audio, self._utterance_id))
            return
        result.events.append(event)
        if kind == "atoms_word":
            self._transcript().add_word(event.get("text", ""))
        elif kind == "atoms_speech_end":
            # Words timed after the audio ended; they ride on the held tail.
            for word in (event.get("rest") or "").split():
                self._transcript().add_word(word)
            self._utterance_id = None
        elif kind == "clear":
            self._barge_in(result)
        elif kind == "atoms_tool_call":
            result.tool_calls.append(
                ToolCall(
                    id=event["id"],
                    name=event["name"],
                    arguments=event.get("arguments") or {},
                    requestor="assistant",
                )
            )
            logger.info(f"atoms tool call: {event['name']}({event.get('arguments')})")
        elif kind == "atoms_closed":
            # Only the bot can close the stream while ticks still run; a call
            # without its agent would otherwise sit silent until tau2's limit.
            code = self._process.poll() if self._process is not None else None
            raise RuntimeError(
                f"atoms bot hung up mid-call (exit code {code}); see {self.call_dir}/atoms_bot.log"
            )

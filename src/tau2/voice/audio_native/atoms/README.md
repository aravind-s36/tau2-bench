# Atoms provider (smallest.ai)

This adapter connects τ-bench's discrete-time audio agent to Atoms, smallest.ai's voice agent: speech recognition, an LLM, speech synthesis and turn-taking in one system.
τ-bench sends the agent the unmodified domain policy as its system prompt and the domain's tool schemas, and nothing else.
Model, speech and turn-taking settings belong to the agent.

## How a call runs

Each call starts one Atoms bot process on this machine and talks to it as a Twilio media stream over a local WebSocket.

1. The adapter writes the call's settings (the policy, the tool schemas, the run's agent settings) to the call's folder and starts the bot from an Atoms checkout.
2. τ-bench's 8 kHz mu-law caller audio goes to the bot as `media` frames, one tick at a time; the bot's audio comes back the same way.
3. The bot sends `clear` when the caller barges in; the adapter drops the bot audio still queued, as the base class does for other providers.
4. Tool calls come from the bot as `atoms_tool_call`; the adapter returns τ-bench's result as `atoms_tool_result`.
5. The bot reports each word as its transport plays it (`atoms_word`) and each utterance's end (`atoms_speech_end`); the adapter releases a word's text only once its audio has played on τ-bench's ticks.
6. After every tick the adapter tells the bot how much of its audio τ-bench has used up, played or dropped at a barge-in (`atoms_played`, in mu-law bytes), so the bot follows τ-bench's ticks rather than the wall clock.

## Configuration

The run is configured through the environment:

| Variable | Description |
|---|---|
| `ATOMS_TAU_REPO` | The Atoms checkout whose `.venv` runs the bot (required) |
| `ATOMS_TAU_OUTPUT` | The folder each call's bot settings, log and traces are written to (required) |
| `ATOMS_TAU_BOT` | JSON of the agent settings every call shares (optional) |
| `ATOMS_TAU_ENV_FILE` | The keys file the bot loads (optional) |
| `ATOMS_TAU_START_SPACING_S` | The least time between two bots starting, 3 s by default |

`--audio-native-model` names the agent's conversational model, `electron-2.0` by default.
`--reasoning-effort` is rejected: reasoning is configured on the agent.

The user simulator generates the caller in real time, as for `openai_live` and `avartha_pipecat`.

## Run

```bash
export ATOMS_TAU_REPO=<atoms checkout> ATOMS_TAU_OUTPUT=<folder for call logs>
tau2 run --audio-native --audio-native-provider atoms --benchmark voice-v1 --speech-complexity regular
```

## Notes for reviewers

The agent behind the adapter is smallest.ai's own code and is not part of this repository; the adapter only starts it and carries audio, tool calls and the events above.

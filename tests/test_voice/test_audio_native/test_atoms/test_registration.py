"""The atoms provider is registered like the other audio-native providers."""

import pytest

from tau2.config import (
    AUDIO_NATIVE_PROVIDER_TYPES,
    DEFAULT_AUDIO_NATIVE_MODELS,
    DEFAULT_AUDIO_NATIVE_REASONING_EFFORT,
)
from tau2.data_model.run_config import AudioNativeConfig
from tau2.voice.audio_native.adapter import create_adapter
from tau2.voice.audio_native.atoms.discrete_time_adapter import (
    DiscreteTimeAtomsAdapter,
)


def test_defaults():
    assert DEFAULT_AUDIO_NATIVE_MODELS["atoms"] == "electron-2.0"
    assert DEFAULT_AUDIO_NATIVE_REASONING_EFFORT["atoms"] is None
    assert AUDIO_NATIVE_PROVIDER_TYPES["atoms"] == "cascaded"


def test_the_user_simulator_generates_the_caller_in_real_time_by_default():
    assert AudioNativeConfig(provider="atoms").realtime_generation_enabled is True


def test_the_factory_builds_the_adapter(monkeypatch, tmp_path):
    monkeypatch.setenv("ATOMS_TAU_REPO", str(tmp_path))
    monkeypatch.setenv("ATOMS_TAU_OUTPUT", str(tmp_path))
    adapter, model = create_adapter(provider="atoms", tick_duration_ms=200)
    assert isinstance(adapter, DiscreteTimeAtomsAdapter)
    assert model == adapter.model == "electron-2.0"


def test_reasoning_effort_is_the_agents_own(monkeypatch, tmp_path):
    monkeypatch.setenv("ATOMS_TAU_REPO", str(tmp_path))
    monkeypatch.setenv("ATOMS_TAU_OUTPUT", str(tmp_path))
    with pytest.raises(ValueError):
        DiscreteTimeAtomsAdapter(tick_duration_ms=200, reasoning_effort="high")

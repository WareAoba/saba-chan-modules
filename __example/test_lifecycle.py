"""템플릿의 정지 요청 방법을 검사한다. 정지 절차(대기·강제 종료·managed)는 공용 graceful_stop이 맡는다."""
import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def lifecycle():
    path = Path(__file__).with_name("__example__lifecycle.py")
    spec = importlib.util.spec_from_file_location("example_lifecycle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _asked(lifecycle, monkeypatch, config, console):
    asked = []
    monkeypatch.setattr(lifecycle, "request_console", lambda config, command: asked.append(command) or console)
    monkeypatch.setattr(lifecycle, "request_terminate", lambda pid: asked.append("SIGTERM") or True)
    monkeypatch.setattr(lifecycle, "graceful_stop", lambda config, request, i18n, module_dir: request())
    return lifecycle.stop(config), asked


def test_stop_asks_the_console_first(lifecycle, monkeypatch):
    assert _asked(lifecycle, monkeypatch, {"instance_id": "example", "pid": 1234}, True) == (True, ["quit"])


def test_without_a_console_only_an_unmanaged_server_gets_sigterm(lifecycle, monkeypatch):
    assert _asked(lifecycle, monkeypatch, {"pid": 1234}, False) == (True, ["quit", "SIGTERM"])
    assert _asked(lifecycle, monkeypatch, {"pid": 1234, "managed": True}, False) == (False, ["quit"])

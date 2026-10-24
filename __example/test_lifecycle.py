"""템플릿의 managed 정지 책임 경계를 검사한다."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def lifecycle():
    path = Path(__file__).with_name("__example__lifecycle.py")
    spec = importlib.util.spec_from_file_location("example_lifecycle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_managed_stop_returns_after_request_without_waiting_or_killing(lifecycle, monkeypatch):
    requests = []

    def request(instance_id, command, *, timeout):
        requests.append((instance_id, command, timeout))
        return {"success": True}

    def unexpected(*args, **kwargs):
        pytest.fail("managed 정지의 대기·강제 종료는 데몬이 맡는다")

    monkeypatch.setattr(lifecycle, "daemon_stdin_command", request)
    monkeypatch.setattr(lifecycle, "wait_for_exit", unexpected)
    monkeypatch.setattr(lifecycle, "force_kill", unexpected)

    result = lifecycle.stop({"managed": True, "instance_id": "example", "pid": 1234})

    assert result["success"] is True
    assert requests == [("example", "quit", 5)]


def test_managed_stop_without_request_path_does_not_signal_server(lifecycle, monkeypatch):
    def unavailable(*args, **kwargs):
        raise ConnectionError("no console")

    def unexpected(*args, **kwargs):
        pytest.fail("managed 정지는 로컬 프로세스에 신호를 보내지 않는다")

    monkeypatch.setattr(lifecycle, "daemon_stdin_command", unavailable)
    monkeypatch.setattr(lifecycle, "os", SimpleNamespace(name="posix", kill=unexpected))
    monkeypatch.setattr(lifecycle, "wait_for_exit", unexpected)
    monkeypatch.setattr(lifecycle, "force_kill", unexpected)

    result = lifecycle.stop({"managed": True, "instance_id": "example", "pid": 1234})

    assert result["success"] is False
    assert result["error_code"] == "graceful_stop_unavailable"


def test_nonmanaged_stop_uses_daemon_timeout(lifecycle, monkeypatch):
    timeouts = []

    def exited(pid, *, timeout):
        timeouts.append(timeout)
        return True

    monkeypatch.setattr(lifecycle, "daemon_stdin_command", lambda *args, **kwargs: {"success": True})
    monkeypatch.setattr(lifecycle, "wait_for_exit", exited)

    result = lifecycle.stop({"managed": False, "instance_id": "example", "pid": 1234,
                             "stop_timeout": 75, "stop_call_limit": 105})

    assert result["success"] is True
    assert timeouts == [75]

"""실측 모드(trial.py)의 판정 규칙. 데몬을 띄우는 전체 흐름은 `--saba-core`를 줄 때만 돈다 (맨 아래)."""
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import zipfile

import pytest

HERE = pathlib.Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("saba_trial", HERE / "trial.py")
trial = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(trial)


def test_console_verbs_come_from_console_commands_and_the_stop_command():
    manifest = {
        "config": {"stop_command": "stop"},
        "commands": {"fields": [
            {"name": "say", "method": "console", "console_template": "say {message}"},
            {"name": "save", "method": "rcon", "console_template": "/save-all flush"},
            {"name": "players", "method": "rest", "endpoint_template": "/players"},
            {"name": "custom", "method": "module"},
            {"name": "raw", "method": "console", "console_template": "{line}"},
        ]},
    }
    assert trial.console_verbs(manifest) == ["save-all", "say", "stop"]


def test_help_gaps_lists_declared_verbs_the_help_never_mentions():
    lines = ["[12:00:00 INFO]: /say <message>", "[12:00:00 INFO]: save-all [flush]", "stopwatch is not stop"]
    assert trial.help_gaps(lines, ["say", "save-all", "stop", "kick", "save"]) == ["kick", "save"]


def test_help_gaps_is_case_insensitive():
    assert trial.help_gaps(["KICK <player>"], ["kick"]) == []


@pytest.mark.parametrize("result, expected", [
    ({"success": False, "error_code": "sandbox_unavailable", "message": "x"}, "sandbox_unavailable"),
    ({"success": False, "error_code": "module_exception", "stderr_tail": ["PermissionError: [WinError 5] Access is denied: 'C:\\x'"]}, "PermissionError"),
    ({"success": False, "message": "msiexec failed with exit code 1601"}, "1601"),
    ({"success": False, "error": "tempfile: [Errno 13] Permission denied"}, "Errno 13"),
    ({"success": False, "error_code": "download_failed", "message": "HTTP 404"}, None),
    ({"success": True, "message": "Access is denied"}, None),
])
def test_sandbox_suspect_flags_isolation_failures_only(result, expected):
    assert trial.sandbox_suspect(result) == expected


def test_snapshot_and_changes(tmp_path):
    (tmp_path / "world").mkdir()
    (tmp_path / "world" / "level.dat").write_text("a")
    (tmp_path / "server.jar").write_text("jar")
    (tmp_path / "backups").mkdir()
    (tmp_path / "backups" / "x.zip").write_text("z")
    (tmp_path / "instance.json").write_text("{}")
    before = trial.snapshot(tmp_path)
    assert set(before) == {"world/level.dat", "server.jar"}
    (tmp_path / "world" / "level.dat").write_text("bb")
    (tmp_path / "world" / "new.dat").write_text("n")
    (tmp_path / "server.jar").unlink()
    after = trial.snapshot(tmp_path)
    assert trial.changed(before, after) == ["world/level.dat", "world/new.dat"]
    assert trial.removed(before, after) == ["server.jar"]


def test_play_data_ignores_logs_and_locks():
    files = ["world/level.dat", "logs/latest.log", "server.log", "world/session.lock", "Saved/Logs/x.txt"]
    assert trial.play_data(files) == ["world/level.dat"]


def _backup_zip(path, manifest_name, entries):
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(manifest_name, json.dumps({"version": 2, "paths": entries, "files": []}))


def test_backup_targets_read_both_manifest_versions(tmp_path):
    _backup_zip(tmp_path / "a.zip", "saba-backup-v2.json", [{"entry": "0", "path": str(tmp_path / "world"), "is_file": False}])
    _backup_zip(tmp_path / "b.zip", "saba-backup.json", [{"entry": "0", "path": str(tmp_path / "ops.json"), "is_file": True}])
    assert trial.backup_targets(tmp_path / "a.zip") == [tmp_path / "world"]
    assert trial.backup_targets(tmp_path / "b.zip") == [tmp_path / "ops.json"]
    (tmp_path / "c.zip").write_text("not a zip")
    assert trial.backup_targets(tmp_path / "c.zip") is None


def test_uncovered_lists_play_data_outside_the_backup(tmp_path):
    targets = [tmp_path / "world", tmp_path / "ops.json"]
    files = ["world/level.dat", "ops.json", "banned.json", "world2/x.dat"]
    assert trial.uncovered(tmp_path, files, targets) == ["banned.json", "world2/x.dat"]


def test_report_counts_and_exit_code():
    report = trial.Report(module="mygame", platform="Windows", isolated=True)
    report.add("boot", "ok", "daemon ready")
    report.add("install", "fail", "download failed", functions=["install_server"], suspect="1601")
    report.add("help", "warn", "missing: kick")
    report.add("reset", "skip", "not started")
    assert report.counts() == {"ok": 1, "fail": 1, "warn": 1, "skip": 1}
    assert report.exit_code() == 1
    text = report.markdown()
    assert "mygame" in text and "install_server" in text and "1601" in text and "AppContainer" in text
    data = json.loads(report.json())
    assert data["steps"][1]["suspect"] == "1601"


def test_report_without_failures_passes():
    report = trial.Report(module="m", platform="Linux", isolated=False)
    report.add("boot", "ok", "")
    report.add("help", "warn", "")
    assert report.exit_code() == 0
    assert "격리 없음" in report.markdown()


# ── 전체 흐름: 데몬과 러너를 실제로 띄운다 ──

def test_trial_runs_the_fixture_module_through_a_real_daemon(pytestconfig, tmp_path):
    saba_core = pytestconfig.getoption("--saba-core")
    if not saba_core:
        pytest.skip("--saba-core <saba-core 실행 파일>을 주면 데몬으로 실측한다")
    out = tmp_path / "report"
    proc = subprocess.run(
        [sys.executable, str(HERE / "trial.py"), str(HERE / "fixtures" / "trialgame"), "--saba-core", saba_core,
         "--out", str(out), "--install-timeout", "300"],
        capture_output=True, text=True, encoding="utf-8", timeout=900,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    report = json.loads((out / "trial-report.json").read_text(encoding="utf-8"))
    steps = {s["name"]: s for s in report["steps"]}
    assert proc.returncode == 0, proc.stdout + proc.stderr + (out / "trial-report.md").read_text(encoding="utf-8")
    for name in ("boot", "install", "settings", "settings-write", "properties", "start", "ready", "stop", "reset"):
        assert steps[name]["status"] == "ok", steps[name]
    # 픽스처 help는 일부러 kick을 빠뜨린다
    assert steps["help"]["status"] == "warn" and "kick" in steps["help"]["detail"]
    # 픽스처는 정지할 때 world/와 함께 백업에 없는 notes.txt도 쓴다
    assert steps["backup"]["status"] == "warn" and "notes.txt" in steps["backup"]["detail"]

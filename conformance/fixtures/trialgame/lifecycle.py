"""픽스처 모듈: dist/의 서버를 "배포판"으로 설치한다 (실측 모드 시험용)"""
import json
import os
import shutil
import sys

from daemon_rcon import atomic_write, run_lifecycle

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER = "trialgame_server.py"
SETTINGS = ("port", "max_players")


def _dir(config):
    return config.get("working_dir") or config.get("install_dir") or ""


def _read(config):
    path = os.path.join(_dir(config), "server.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def list_versions(config):
    return {"success": True, "versions": [{"id": "1.0.0", "version": "1.0.0"}], "latest": {"release": "1.0.0"}}


def install_server(config):
    install_dir = config.get("install_dir")
    if not install_dir:
        return {"success": False, "message": "install_dir not specified"}
    os.makedirs(install_dir, exist_ok=True)
    shutil.copy(os.path.join(MODULE_DIR, "dist", SERVER), os.path.join(install_dir, SERVER))
    with open(os.path.join(install_dir, "version.txt"), "w", encoding="utf-8") as f:
        f.write(config.get("version") or "1.0.0")
    initial = {k: v for k, v in (config.get("settings") or {}).items() if k in SETTINGS}
    atomic_write(os.path.join(install_dir, "server.json"), json.dumps(initial).encode())
    return {"success": True, "version": config.get("version") or "1.0.0", "install_path": install_dir}


def get_installed_version(config):
    path = os.path.join(_dir(config), "version.txt")
    if not os.path.exists(path):
        return {"success": False, "message": "not installed"}
    with open(path, encoding="utf-8") as f:
        return {"success": True, "version": f.read().strip()}


def get_launch_command(config):
    working_dir = _dir(config)
    server = os.path.join(working_dir, SERVER)
    if not os.path.exists(server):
        return {"success": False, "action_required": "executable_not_found", "message": f"Executable not found: {server}"}
    return {"success": True, "program": sys.executable, "args": [server], "working_dir": working_dir, "env_vars": {}}


def validate(config):
    return {"success": True, "issues": []}


def status(config):
    return {"success": True, "status": "unknown"}


def configure(config):
    current = _read(config)
    current.update({k: v for k, v in (config.get("settings") or {}).items() if k in SETTINGS})
    atomic_write(os.path.join(_dir(config), "server.json"), json.dumps(current).encode())
    return {"success": True}


def import_settings(config):
    return {"success": True, "settings": {k: v for k, v in _read(config).items() if k in SETTINGS}}


def read_properties(config):
    return {"success": True, "properties": _read(config)}


def reset_server(config):
    shutil.rmtree(os.path.join(_dir(config), "world"), ignore_errors=True)
    return {"success": True}


def diagnose_log(config):
    return {"success": True, "issues": []}


FUNCTIONS = {
    "list_versions": list_versions,
    "install_server": install_server,
    "get_installed_version": get_installed_version,
    "get_launch_command": get_launch_command,
    "validate": validate,
    "status": status,
    "configure": configure,
    "import_settings": import_settings,
    "read_properties": read_properties,
    "reset_server": reset_server,
    "diagnose_log": diagnose_log,
}

if __name__ == "__main__":
    run_lifecycle(FUNCTIONS)

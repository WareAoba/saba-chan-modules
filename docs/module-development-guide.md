# 사바쨩 모듈 개발 가이드

모듈은 게임 서버 하나를 사바쨩 데몬이 다룰 수 있게 해 주는 "설명서"다. 이 문서는 모듈을 **만드는 순서와 요령**을 다룬다. 키와 함수의 정확한 목록은 [module-spec.md](module-spec.md)가 기준이고, 여기서는 그 표를 되풀이하지 않는다.

참고할 실제 모듈:

| 모듈 | 볼 만한 점 |
|---|---|
| `minecraft` | 콘솔(stdin) 명령, `download` 설치, 백업, 공지, 버전 목록 |
| `zomboid` | 콘솔 명령, `steamcmd` 설치, 인스턴스별 데이터 폴더(`cache_dir`) |
| `palworld` | 콘솔이 없는 게임: REST 명령, `[credential_map]`, `ue4-ini` 익스텐션 |
| `__example` | 주석이 달린 빈 틀 (`__example__module.toml`, `__example__lifecycle.py`) |

## 1. 보안 정책: RCON은 쓰지 않는다

- 게임 명령은 전부 데몬을 거쳐 **서버 콘솔(stdin)** 또는 게임의 **REST API**로 간다. `[protocols] supported`에는 `"stdin"`과 `"rest"`만 쓴다.
- 게임에 RCON 설정이 있으면 모듈이 실행 전·설정 저장 때마다 **끈다**. 예: minecraft `_enforce_rcon_policy()`가 `enable-rcon=false`, palworld는 `RCONEnabled=False`, zomboid는 `RCONPassword`를 비운다.
- RCON 포트·비밀번호를 설정 화면에 두지 않는다. 데몬은 예전 인스턴스 파일에 남은 RCON 키를 읽을 때 지운다.
- 공용 도우미 파일 이름은 `daemon_rcon.py`지만 이름만 남은 것이다. 실제로는 데몬 API와 프로세스 처리 도우미다(§5).

## 2. 데몬과 모듈이 주고받는 방식

```
데몬 ──(stdin: config JSON)──▶ python lifecycle.py <함수 이름> ──(stdout 마지막 줄: 결과 JSON)──▶ 데몬
                                         └─ stderr: 로그, PROGRESS:{json} 진행 상황
```

- 함수마다 새 Python 프로세스가 뜬다. 모듈은 상태를 메모리에 들고 있지 않는다. 필요한 값은 매번 config나 게임 설정 파일에서 읽는다.
- 답은 늘 JSON 객체이고 `success: bool`을 넣는다. 실패해도 **종료 코드 0**으로 끝낸다(0이 아니면 데몬이 stdout을 버린다).
- 로그는 `print(..., file=sys.stderr)`로만 쓴다. stdout에 찍으면 결과 JSON이 깨질 수 있다.
- 데몬은 `PYTHONPATH`에 공용 폴더(`i18n.py`, `daemon_rcon.py`)와 익스텐션 폴더를 넣고, `SABA_IPC_URL`, `SABA_TOKEN_PATH`, `SABA_LANG`을 준다. 공용 파일을 모듈 폴더에 복사하지 않는다.

### 서버 실행: managed가 기본

모든 서버는 기본으로 **managed**다. saba-runner가 서버를 띄우고 PID, 콘솔 입출력, 종료 코드를 쥔다. 데몬이 재시작해도 콘솔(stdin)은 이어진다.

- 데몬이 `get_launch_command`를 부르고, 받은 `program`/`args`/`working_dir`/`env_vars`로 서버를 띄운다. 모듈은 프로세스를 직접 띄우지 않는다.
- 인스턴스 설정 `managed_start = false`면 non-managed다. 이때도 데몬이 같은 `get_launch_command`로 띄우고, 정지·상태는 모듈의 `stop`/`status`에 맡긴다.
- config의 `managed`(bool)로 지금 어느 쪽인지 알 수 있다.

### 데몬이 함수를 부르는 때

| 때 | 함수 |
|---|---|
| 서버 시작 전 점검 (critical 문제가 있으면 시작하지 않는다) | `validate` |
| 서버 시작 | `get_launch_command` |
| 정지: 콘솔이 있는 managed 서버는 데몬이 `[config] stop_command`를 stdin으로 보낸다. 콘솔이 없거나 non-managed면 | `stop` |
| non-managed 상태 조회 | `status` |
| `method = "module"` 명령 | `command` |
| REST 명령 전에 접속 정보가 필요할 때 | `get_credentials` |
| 설정 화면 저장 / 읽기 / 초기화 | `configure` / `read_properties`, `import_settings` / `reset_properties`, `reset_server` |
| 기존 서버 가져오기 | `import_settings` |
| 설치·업데이트 | `list_versions`, `get_version_details`, `install_server`, `get_installed_version` |
| 기타 | `accept_eula`, `diagnose_log`, `backup_paths`(`[backup] hook = true`일 때) |

필수는 `get_launch_command` 하나다. 나머지는 지원할 기능만 만들고, 만들었으면 `FUNCTIONS` 표에 꼭 넣는다(적합성 키트가 검사한다).

## 3. 빠른 시작

1. 폴더를 만든다. 폴더 이름 = `[module] name` (영문 소문자).
   ```
   mygame/
     module.toml
     lifecycle.py
     locales/en.json   (선택, 화면 문구)
     icon.png          (선택)
   ```
2. `__example/__example__module.toml`을 `module.toml`로, `__example/__example__lifecycle.py`를 `lifecycle.py`로 복사하고 필요 없는 부분을 지운다.
3. 최소 `module.toml`:
   ```toml
   [module]
   schema_version = 1
   name = "mygame"
   version = "0.1.0"
   entry = "lifecycle.py"

   [protocols]
   supported = ["stdin"]
   default = "stdin"
   interaction_mode = "console"

   [config]
   server_executable = "GameServer.exe"
   default_port = 7777
   stop_command = "quit"
   ```
4. 최소 `lifecycle.py`:
   ```python
   import os
   from daemon_rcon import config_value, run_lifecycle

   def get_launch_command(config):
       exe = config.get("server_executable", "")
       if not exe or not os.path.exists(exe):
           return {"success": False, "message": f"Server executable not found: {exe}"}
       port = config_value(config, "port", 7777)
       return {
           "success": True,
           "program": exe,
           "args": [f"-port={port}"],
           "working_dir": config.get("working_dir") or os.path.dirname(exe),
           "env_vars": {},
       }

   FUNCTIONS = {"get_launch_command": get_launch_command}

   if __name__ == "__main__":
       run_lifecycle(FUNCTIONS)
   ```
   `run_lifecycle()`이 stdin 읽기, 모르는 함수 답, 예외를 JSON으로 바꾸기, 종료 코드 0을 처리한다.
5. 데몬 없이 돌려 본다.
   ```bash
   echo '{"server_executable": "C:/Games/MyGame/GameServer.exe", "port": 7777}' | python lifecycle.py get_launch_command
   ```
   (`daemon_rcon`을 찾으려면 `PYTHONPATH`에 saba-chan 저장소의 `modules/`를 넣는다.)
6. 검사한다(§7).

그 다음은 필요한 만큼 늘린다: `validate` → 설정 화면(`[[settings.fields]]` + `configure`/`import_settings`) → 명령(`[[commands.fields]]`) → 설치(`[install]`) → 크래시 판정(`[detection]`) → 백업·공지.

## 4. lifecycle.py 작성 요령

### config에서 값 읽기

데몬은 인스턴스 설정(`module_settings`)을 config 최상위에 펼쳐 넣는다. 함수에 따라 `settings` 안에 들어오기도 하므로 `daemon_rcon.config_value(config, key, default)`로 읽는다(최상위 → `module_settings` → `settings` 순서, 빈 값은 없는 값).

자주 오는 키: `instance_id`, `server_executable`, `working_dir`, `port`, `managed`, `protocol_mode`, `server_version`, `extension_data`, REST 게임이면 `rest_host`·`rest_port`·`rest_username`·`rest_password`. `stop`에는 `pid`·`force`, `command`에는 `command`·`args`, `configure`에는 `settings`가 더해진다.

### validate

```python
def validate(config):
    issues = []
    exe = config.get("server_executable", "")
    if not exe or not os.path.exists(exe):
        issues.append({"code": "EXECUTABLE_NOT_FOUND", "severity": "critical", "message": "..."})
    return {"success": not any(i["severity"] == "critical" for i in issues), "issues": issues}
```

`severity: "critical"`이 하나라도 있으면 데몬이 서버를 켜지 않고 그 메시지를 보여 준다. `"warning"`은 표시만 한다.

### stop: 정상 종료 → 기다림 → 강제 종료

콘솔이 있는 managed 서버는 데몬이 `stop_command`를 보내고 `[config] stop_timeout`(기본 30초)만큼 기다린 뒤 강제 종료한다. 모듈의 `stop`은 그 밖의 경우에 불리므로 이 순서를 지킨다.

1. `force`가 아니면 정상 종료를 요청한다: `daemon_stdin_command(instance_id, "stop")`로 콘솔에 종료 명령, 콘솔이 없으면 POSIX에서 `SIGTERM`.
2. `wait_for_exit(pid, timeout=30)`으로 기다린다.
3. 그래도 안 끝났거나 `force`면 `force_kill(pid)`.

```python
from daemon_rcon import daemon_stdin_command, force_kill, wait_for_exit

def stop(config):
    pid = config.get("pid")
    if not pid:
        return {"success": False, "message": "No PID provided"}
    if not config.get("force"):
        try:
            daemon_stdin_command(config.get("instance_id"), "quit", timeout=5)
            if wait_for_exit(pid, timeout=30):
                return {"success": True, "message": "Stopped"}
        except Exception as e:  # 콘솔 없음, 데몬 없음
            print(f"console stop unavailable: {e}", file=sys.stderr)
    ok, error = force_kill(pid)
    return {"success": ok, "message": "Force killed" if ok else str(error)}
```

프로세스 확인과 종료는 직접 만들지 말고 `is_process_alive`, `wait_for_exit`, `force_kill`을 쓴다. Windows에서 `os.kill(pid, 0)`은 프로세스를 끝내 버린다. 실행 파일 이름으로 프로세스를 찾아 죽이지 않는다: 같은 게임의 다른 인스턴스까지 끝난다. 데몬이 넘긴 `pid`만 다룬다. 실제 구현은 minecraft `stop()`(콘솔 `stop` → SIGTERM → 대기 → 강제 종료)을 본다.

### 설정 화면: configure / import_settings

- `module.toml` `[[settings.fields]]`의 `name`과 게임 설정 파일의 키를 표 하나(`_KEY_MAP` 같은 dict)로 잇고, `configure`(사바쨩 → 파일)와 `import_settings`(파일 → 사바쨩) 모두 이 표를 쓴다. 키가 늘면 표에 한 줄만 더한다.
- 파일에서 읽은 문자열은 `load_setting_types(MODULE_DIR)` + `coerce_setting(value, type)`으로 필드 타입에 맞춘다(비밀번호처럼 숫자로 보이는 문자열은 바꾸지 않는다).
- 목록 값처럼 게임 형식이 따로 있는 값은 쓸 때 바꾼다. 예: palworld `_LIST_KEYS`가 `PALBOX, RepairBench`를 `("PALBOX","RepairBench")`로 쓴다.
- RCON 설정은 여기서도 늘 끈다(§1).

### 명령: 선언으로 먼저, 코드는 마지막

`[[commands.fields]]`의 `method`로 고른다.

| 게임 | method | 데몬이 하는 일 |
|---|---|---|
| 콘솔(stdin)로 명령을 받는다 | `"console"` + `console_template` | `{입력}` 자리를 채워 콘솔에 한 줄 보내고 출력을 잠깐 모아 돌려준다. lifecycle.py는 거치지 않는다 |
| REST API가 있다 | `"rest"` + `http_method`, `endpoint_template` | 게임 REST를 직접 부른다. 접속 정보는 `get_credentials`나 `[credential_map]`으로 |
| 둘 다 아니다 (여러 단계, 파일 조작 등) | `"module"` (기본) | lifecycle.py `command(config)`를 부른다 |

```toml
[[commands.fields]]
name = "say"
risk = "safe"
label = "Send Chat Message"
description = "Broadcast a message to the server"
method = "console"
console_template = "say {message}"
inputs = [ { name = "message", type = "text", label = "Message", required = true } ]
```

- `risk`(`safe`/`caution`/`danger`)와 `description`은 AI 명령 해석이 읽는다. 서버 종료·추방·권한 변경은 `danger`.
- 데몬이 `inputs`의 required·min·max·options를 실행 전에 검사하므로 `command`에서 다시 검사하지 않아도 된다.
- `command`에서 콘솔에 보내야 하면 `daemon_stdin_command(config["instance_id"], line)`.
- 예전 이름 `method = "rcon"` / `rcon_template`도 읽히지만 새 모듈은 `console` / `console_template`을 쓴다.

### 설치

- `[install] method = "download"`: 모듈의 `list_versions` / `install_server`가 내려받는다(minecraft).
- `method = "steamcmd"` 같은 익스텐션 방식: 익스텐션이 설치를 맡고, 모듈은 `[install] requires_extensions`와 `[module.dependencies] ext-<id>`로 필요를 선언한다(zomboid, palworld).
- 익스텐션은 `import extensions.<이름>`으로 쓰되 함수 안에서 import하고, 없으면 죽지 말고 `error_code: "extension_required"`, `missing_extensions: [...]`로 답한다(palworld `ExtensionMissing`).

### 크래시 판정, 백업, 공지

- `[detection] graceful_exit_patterns`(정상 종료 문구)와 `crash_artifacts`(크래시 때 생기는 파일)를 적어야 데몬이 크래시만 골라 자동 재시작한다. 판정 순서는 module-spec.md `[detection]`에 있다.
- `[backup] paths`(작업 폴더 기준, `{설정 이름}` 치환), `save_command`(백업 직전 보낼 `[commands.fields]` 이름), 경로를 코드로 계산해야 하면 `hook = true` + `backup_paths(config)` → `{"success": true, "paths": [...]}`. 훅이 실패하면 백업도, 그 백업을 전제로 한 업데이트·복원도 멈춘다.
- `[config] announce_command`: 예약 재시작 예고·공지에 쓸 `[commands.fields]` 이름(`message` 입력 하나).

## 5. 공용 도우미 (`daemon_rcon.py`, 데몬이 배포)

| 함수 | 용도 |
|---|---|
| `daemon_stdin_command(instance_id, line, timeout=10)` | 데몬이 관리하는 서버 콘솔에 한 줄 보내기 |
| `daemon_post(path, payload, timeout=10)` | 그 밖의 데몬 API 호출 (토큰 자동) |
| `is_process_alive(pid)`, `wait_for_exit(pid, timeout)`, `force_kill(pid, tree=True)` | PID 단위 프로세스 확인·대기·강제 종료 (Windows/POSIX 공용) |
| `config_value(config, key, default)` | config 세 위치에서 설정 값 찾기 |
| `load_setting_types(module_dir)`, `coerce_setting(value, type)` | 파일 값을 필드 타입으로 |
| `run_lifecycle(FUNCTIONS)` | `python lifecycle.py <함수>` 진입점 |

`daemon_rcon_command`, `DaemonRconClient`, `rcon_command`는 예전 코드 호환용이다. 이름과 달리 이것들도 콘솔로 보내지만, 새 모듈은 `daemon_stdin_command`를 쓴다.

## 6. 화면 문구 (i18n)

```python
from i18n import I18n
i18n = I18n(MODULE_DIR)            # MODULE_DIR/locales/<언어>.json
i18n.t("errors.failed_to_stop", error=str(e))   # 문구 안의 {{error}}가 바뀐다
```

언어는 `SABA_LANG`(GUI 언어), 없으면 `en`. 키가 없으면 `en` 문구, 그것도 없으면 키 자체가 나온다. `locales/en.json`이 기준이고 다른 언어 파일에 en에 없는 키를 두지 않는다.

## 7. 검사

```bash
# 데몬과 같은 파서로 module.toml 검사 (실패하면 종료 코드 1)
saba-core module validate mygame

# 적합성 키트: 스키마, FUNCTIONS 표, 모르는 함수 답, 안전한 호출의 JSON 계약, 번역 키
pytest conformance --module path/to/mygame --shared-libs path/to/saba-chan/modules
```

키트는 데몬과 익스텐션 없이 함수를 부른다. 데몬에 닿지 않아도, 서버 파일이 없어도 모듈은 JSON으로 답해야 한다. 자세한 검사 목록은 [conformance/README.md](../conformance/README.md).

게임 동작은 모듈 폴더의 `test_lifecycle.py`(pytest)로 시험한다. 실제 게임 서버 없이 함수에 config를 넣고 답과 파일 변화를 본다(minecraft, palworld, zomboid의 `test_lifecycle.py` 참고).

## 8. 체크리스트

- [ ] 폴더 이름 = `[module] name`, `schema_version = 1`, `saba-core module validate` 통과
- [ ] `get_launch_command`가 진짜 서버 exe를 돌려준다(곧바로 끝나는 런처 exe가 아니라)
- [ ] `FUNCTIONS`에 만든 함수가 다 있고, 모든 실패가 종료 코드 0 + `success: false` + 이유
- [ ] 로그는 stderr로만
- [ ] RCON은 끄고, `supported`에 `"rcon"`이 없다
- [ ] 콘솔이 있으면 `stop_command`, `stop`은 정상 종료 → 대기 → 강제 종료 순서, 데몬이 준 `pid`만 다룬다
- [ ] 명령은 `console` / `rest` 선언으로 되는 것은 선언으로, `risk`와 `description`을 적었다
- [ ] 크래시 흔적(`crash_artifacts`)과 정상 종료 문구(`graceful_exit_patterns`)를 적었다
- [ ] 익스텐션이 없을 때 `extension_required`로 답한다
- [ ] `pytest conformance --module ...` 통과

# 사바쨩 모듈 개발 가이드

모듈은 게임 서버 하나를 사바쨩 데몬이 다룰 수 있게 해 주는 "설명서"다. 이 문서는 모듈을 **만드는 순서와 요령**을 다룬다. 키와 함수의 기준은 saba-chan의 [모듈 계약](https://github.com/WareAoba/saba-chan/blob/main/docs/architecture/02-modules.md)이다. 여기서는 그 표를 되풀이하지 않는다.

참고할 실제 모듈:

| 모듈 | 볼 만한 점 |
|---|---|
| `minecraft` | 콘솔(stdin) 명령, `download` 설치, 백업, 공지, 버전 목록 |
| `zomboid` | 콘솔 명령, `steamcmd` 설치, 인스턴스별 데이터 폴더(`cache_dir`) |
| `palworld` | 콘솔이 없는 게임: REST 명령, `[credential_map]`, `ue4-ini` 익스텐션 |
| `__example` | 주석이 달린 빈 틀 (`__example__module.toml`, `__example__lifecycle.py`) |
| `terraria` | 공식 zip 설치(OS 폴더 고르기), cp1252 설정 파일 보존, 포터블 런타임(XNA), 의사 콘솔(`console = "pty"`), 언어별 콘솔 문구 |

## 0. 만들기 전에: 조사와 실측

모듈이 틀리는 가장 흔한 이유는 문서나 기억으로 값을 채우는 것이다. 테라리아 모듈을 문서만 보고 만들었을 때, 단위 테스트와 적합성 검사는 모두 통과했지만 실제 서버에서는 결함이 9가지 나왔다(아래 표). 그래서 코드를 쓰기 전에 게임을 조사하고, 실제 서버를 받아 실행해 본다.

### 0.1 조사 표를 만든다

`<game>/RESEARCH.md`에 표를 만든다. 항목마다 출처를 붙이고, 출처가 "문서"인지 "실측"인지 적는다. 출처를 찾지 못한 항목은 비워 두고 "확인 못 함"으로 적는다.

| 조사할 것 | 찾을 곳 |
|---|---|
| 실제 서버 실행 파일, 실행 인자, 기본 포트 | 공식 문서, 서버 `--help`, 동봉 실행 스크립트 |
| 명령 전체와 인자 | 공식 명령 문서와 서버 `help` 출력. 둘을 한 줄씩 대조한다 |
| 설정 전체(키, 형식, 범위, 기본값) | 서버가 처음 만든 기본 설정 파일, 없으면 배포물에 동봉된 예시 파일 |
| 설정 파일의 문법과 인코딩 | 그 파일의 원래 바이트 |
| 세이브 위치, 게임이 만드는 회전 백업 | 서버를 실행하고 저장한 뒤 생긴 파일 목록 |
| 준비 완료·정상 종료·접속·퇴장 문구 | 서버 콘솔 원문. 서버 언어 설정이 문구를 번역하는지도 본다 |
| 설치 방법과 검증 수단 | 공식 내려받기 주소, 공식 체크섬(없으면 "없음"과 근거) |
| 실행 전제(런타임) | 실행 파일의 참조 목록, 공식 문서. 배포물에 없는 것을 찾는다 |
| 인스턴스 폴더 밖에 쓰는 파일 | 실행 뒤 홈 폴더·문서 폴더 변화 |

위키·커뮤니티 자료는 옛 버전 기준일 수 있다. 테라리아 위키의 `death` 명령은 1.4.5.8 서버에서 "Invalid command."였다.

### 0.2 실물을 열어 본다

서버를 받을 수 있으면 받아서 아래를 직접 확인한다.

1. 배포물의 항목 전체를 출력한다. 여러 OS 폴더가 있는지, 같은 실행 파일 이름이 여러 폴더에 있는지 본다. 테라리아 zip은 `Linux/` 폴더에도 `TerrariaServer.exe`(Mono용)가 있다.
2. 동봉 기본 파일을 바이트 단위로 본다. 인코딩(UTF-8, BOM, cp1252), 줄바꿈(LF, CRLF), 주석 처리된 예시 키, 주석이 아닌 줄을 본다. 주석 처리된 예시 키도 설정 목록에 넣는다. 주석이 아닌 줄의 값이 기본값이다.

### 0.3 서버를 실행해 본다

서버를 데몬과 같은 방식(stdin·stdout 파이프)으로 띄우고 원문을 기록한다. 제작 환경에서 다른 OS용 빌드밖에 실행할 수 없어도 실행한다. 다만 이것은 부분 실측이다. 게임 코드가 같으면 명령 이름, `help` 출력, 문구, 저장 동작은 그대로 옮겨 쓸 수 있다. 콘솔 입력 방식(파이프·pty), 런타임 전제 조건, 폴더 구조, 인코딩, 샌드박스 안 동작은 대상 OS에서 다시 실측해야 한다. 테라리아 Windows 서버는 파이프로 넣은 stdin을 무시한다(§0.4). 대상 OS에서 실측하기 전까지는 그 OS의 콘솔 동작을 "미검증"으로 적어 둔다.

- 준비 완료 줄, 정상 종료 명령 뒤의 줄, 저장에 걸린 시간
- `help` 출력. 문서에만 있는 명령은 실제로 보내 결과를 본다
- 콘솔 프롬프트가 다음 줄 앞에 붙는지. 테라리아는 프롬프트 `: `를 줄바꿈 없이 찍어 `: Server started`처럼 붙는다
- 파이프로 보낸 명령에 서버가 답하는지. 답하지 않으면 의사 콘솔(§4 "콘솔 방식")로 다시 잰다
- 서버 언어 설정마다 바뀌는 문구
- 실행·저장 뒤 생긴 파일(회전 백업 `world.wld.bak`, `world.wld.bak2` 같은 것)

게임 클라이언트가 있어야 재는 항목(접속·퇴장 문구, 추방 대상)은 비워 두고 "실측 못 함"으로 적는다.

### 0.4 테라리아에서 실제로 나온 결함

| 결함 | 원인 | 막는 방법 |
|---|---|---|
| Windows에 설치하면 Linux 서버가 풀림 | "실행 파일이 든 가장 짧은 경로"로 폴더를 고름 | OS 폴더 이름으로 고른다 (§4 "설치") |
| 공식 설정 파일이 있으면 시작·저장·가져오기가 모두 실패 | 설정 파일을 UTF-8로만 읽음. 실제 파일은 cp1252 | 읽은 인코딩으로 다시 쓴다 (§4 "설정 파일의 형식") |
| 명령 4개가 서버에 없고 1개가 빠짐 | 위키 목록을 그대로 옮김 | `help` 출력과 대조한다 (§0.3) |
| 준비 표시가 없음 | 준비 문구를 넣지 않음. 넣어도 프롬프트 접두와 번역 때문에 맞지 않음 | 원문으로 패턴을 쓴다 (§4 "콘솔 패턴") |
| 초기화 뒤에도 옛 월드가 남음 | 회전 백업 `.bak2`를 지우지 않음 | 실행 뒤 생긴 파일로 대상을 정한다 |
| Windows 서버가 시작 즉시 죽음 | XNA Framework 4.0이 없음 | 고정 판 런타임을 서버 폴더에 둔다 (§4 "실행 전제") |
| 명령·`exit`가 먹지 않고 정지가 시간 초과로 강제 종료 | Windows 서버가 파이프 입력을 읽지 않음 | `[launch] console = "pty"` (§4 "콘솔 방식") |
| 격리 실행에서 설정 파일 저장 실패 | 공용 `atomic_write`가 기존 파일을 바꾸지 못함(데몬 0.1.10에서 고침) | PC 실측을 격리 실행으로 한다 (§7) |
| 격리 실행에서 XNA 설치 실패 | `msiexec`(1601)과 `tempfile.mkdtemp` 폴더를 쓸 수 없음 | 격리 실행 제약을 지킨다 (§4 "격리 실행") |

## 1. 통신 경로: STDIN → REST → RCON

- 게임 명령은 전부 데몬을 거친다. 게임이 지원하는 것 중 **서버 콘솔(stdin)**을 먼저, 없으면 게임의 **REST API**, 둘 다 없을 때만 **RCON**을 쓴다. 다른 경로가 있으면 RCON은 쓰지 않는다(`supported`에 넣지 않는다).
- 콘솔이나 REST가 있는 게임에 RCON 설정이 있으면 모듈이 실행 전·설정 저장 때마다 **끈다**. 예: minecraft `_enforce_rcon_policy()`가 `enable-rcon=false`, palworld는 `RCONEnabled=False`, zomboid는 `RCONPassword`를 비운다.
- **RCON만 받는 게임**(`supported = ["rcon"]`): `[protocols] rcon_port`(게임의 기본 RCON 포트)가 필수다. 데몬이 처음 시작할 때 인스턴스마다 포트와 무작위 비밀번호를 정해 인스턴스 폴더 `rcon.json`에 두고, 모듈 config에 `rcon_port`·`rcon_password`로 넘긴다. 모듈은 `get_launch_command`(또는 실행 전 준비)에서 이 값을 게임 설정에 쓰고, RCON을 127.0.0.1에서만 받게 한다. 데몬은 `127.0.0.1:<rcon_port>`에만 접속한다.
  - 콘솔 명령(`method = "console"`)과 정의에 없는 한 줄은 데몬이 RCON으로 보낸다. 모듈이 RCON 소켓을 직접 열지 않는다.
  - 정지는 데몬이 RCON으로 `stop_command`를 보내지 않는다. 모듈 `stop()`이 데몬 `/api/instance/<id>/rcon`에 정지 명령을 보낸다: `daemon_post(f"/api/instance/{instance_id}/rcon", {"command": "<정지 명령>"})`. 보낼 수 없으면 `graceful_stop_unavailable`(§3 stop).
  - RCON 포트·비밀번호를 설정 화면(`[[settings.fields]]`)에 두지 않는다. 비밀번호를 로그·응답·오류 문구에 쓰지 않는다.
- 공용 도우미 파일 이름은 `daemon_rcon.py`지만 RCON 클라이언트가 아니다. 데몬 API와 프로세스 처리 도우미다(§5).

## 2. 데몬과 모듈이 주고받는 방식

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/module-call-dark.svg">
  <img src="images/module-call-light.svg" alt="데몬이 python lifecycle.py &lt;함수 이름&gt;을 띄워 stdin으로 설정 JSON을 넣고, stdout에서 결과 JSON을, stderr에서 로그와 PROGRESS 진행 상황을 받는다.">
</picture>

- 함수마다 새 Python 프로세스가 뜬다. 모듈은 상태를 메모리에 들고 있지 않는다. 필요한 값은 매번 config나 게임 설정 파일에서 읽는다.
- 답은 늘 JSON 객체이고 `success: bool`을 넣는다. 실패해도 **종료 코드 0**으로 끝낸다(0이 아니면 데몬이 stdout을 버린다).
- 로그는 `print(..., file=sys.stderr)`로만 쓴다. stdout에 찍으면 결과 JSON이 깨질 수 있다.
- 데몬은 `PYTHONPATH`에 공용 폴더(`i18n.py`, `daemon_rcon.py`)와 익스텐션 폴더를 넣는다. `SABA_LANG`은 호출 언어다. 공용 파일을 모듈 폴더에 복사하지 않는다.
- 0.1.10부터 모든 Python 호출에 `SABA_HOST_DIR`을 준다. SDK가 이 통로로 인스턴스의 stdin·REST 요청을 보낸다. 모듈은 HOST 파일이나 IPC 토큰을 직접 다루지 않는다. 기존 HTTP 연동용 `SABA_IPC_URL`·`SABA_TOKEN_PATH`는 호환용이며, 격리 실행에는 토큰 경로를 주지 않는다.
- 결과 stdout의 상한은 4 MiB다. 일반 stderr는 마지막 200줄, 합계 64 KiB만 보존한다. 일반 호출은 최대 8개, `install_server`는 별도로 최대 2개가 동시에 실행된다. 기본 기한은 120초이고 설치는 1800초다. 자리 대기도 기한에 들지만 격리 실행 준비는 빠진다. 전체 규칙과 오류 필드는 [실행기 계약](https://github.com/WareAoba/saba-chan/blob/main/docs/architecture/01-daemon-core.md#71-srcpluginmodrs-python-모듈익스텐션-실행기-단발-프로세스)을 따른다.

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
| 기타 | `accept_agreements`(`[[agreements]]`를 선언했을 때), `diagnose_log`, `backup_paths`(`[backup] hook = true`일 때), `instance_commands`(`[commands] hook = true`일 때) |
| 조회·접속자 수 | `[[queries]]`의 `function`, `[status] players`가 가리키는 함수 (이름은 module.toml이 정한다) |

필수는 `get_launch_command` 하나다. 나머지는 지원할 기능만 만들고, 만들었으면 `FUNCTIONS` 표에 꼭 넣는다. 훅을 켰거나 module.toml이 이름으로 가리킨 함수는 반드시 만든다(적합성 키트가 둘 다 검사한다).

## 3. 빠른 시작

1. 폴더를 만든다. 폴더 이름 = `[module] name` (영문 소문자).
   ```
   mygame/
     module.toml
     lifecycle.py
     locales/en.json   (선택, 화면 문구)
     icon.png          (선택)
   ```
2. `__example/__example__module.toml`을 `module.toml`로, `__example/__example__lifecycle.py`를 `lifecycle.py`로 복사한다. `__example/locales/`도 복사하고 필요 없는 부분을 지운다.
3. 최소 `module.toml`:
   ```toml
   [module]
   schema_version = 1
   name = "mygame"
   version = "0.1.0"
   entry = "lifecycle.py"

   [module.dependencies]
   saba-core = ">=0.1.2"

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
   `run_lifecycle()`이 stdin 읽기, 모르는 함수 답, 예외를 JSON으로 바꾸기, 종료 코드 0, 그리고 데몬이 지원 기능을 묻는 예약 함수 `functions`의 답을 처리한다. 진입점을 직접 쓰면 `functions`도 직접 답해야 한다.
5. 데몬 없이 돌려 본다.
   ```bash
   echo '{"server_executable": "C:/Games/MyGame/GameServer.exe", "port": 7777}' | python lifecycle.py get_launch_command
   ```
   (`daemon_rcon`을 찾으려면 `PYTHONPATH`에 saba-chan 저장소의 `modules/`를 넣는다.)
6. 검사한다(§7).

그 다음은 필요한 만큼 늘린다: `validate` → 설정 화면(`[[settings.fields]]` + `configure`/`import_settings`) → 명령(`[[commands.fields]]`) → 설치(`[install]`) → 크래시 판정(`[detection]`) → 백업·공지 → 조회·접속자 수·약관·프리셋(§4 "선택 기능"). `__example__module.toml`의 "선택 기능" 절에 주석으로 된 예가 있다.

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

### stop: managed는 요청 뒤 반환

콘솔이 있는 managed 서버는 데몬이 `stop_command`를 보내고 기다린다. stdin이 없거나 REST로 정지하는 managed 서버는 모듈의 `stop`을 부른다. 이때 모듈은 저장·종료 요청을 보낸 뒤 바로 `success: true`로 답한다. 서버가 아직 살아 있어도 요청이 접수됐으면 성공이다. 종료 대기와 강제 종료는 데몬이 맡는다.

요청할 길이 없으면 서버를 끝내지 않고 `{"success": false, "error_code": "graceful_stop_unavailable", "message": <모듈 locales>}`로 답한다. 데몬은 기다리거나 강제 종료하지 않고 사용자에게 알린다. 사용자가 강제 종료를 고르면 데몬이 처리한다.

`managed: false`일 때만 모듈이 정상 종료 요청, `wait_for_exit`, `force_kill` 순서를 맡는다. 콘솔·REST가 없으면 POSIX의 `SIGTERM`을 시도할 수 있다. 정상 종료를 요청할 길이 없으면 여기서도 `graceful_stop_unavailable`로 답한다. 대기 시간은 데몬이 준 `stop_timeout`을 쓴다. `stop_call_limit`은 함수 호출 전체의 제한이다. 이 키가 없는 옛 데몬에서는 호출이 120초 안에 끝나게 한다. `force: true`인 non-managed 정지는 바로 강제 종료한다.

```python
from daemon_rcon import daemon_stdin_command, force_kill, wait_for_exit

def stop(config):
    pid = config.get("pid")
    managed = config.get("managed", False)
    if not pid:
        return {"success": False, "message": "No PID provided"}
    if managed or not config.get("force"):
        try:
            daemon_stdin_command(config.get("instance_id"), "quit", timeout=5)
        except (ConnectionError, TimeoutError) as e:
            print(f"console stop unavailable: {e}", file=sys.stderr)
            return {"success": False, "error_code": "graceful_stop_unavailable",
                    "message": i18n.t("errors.graceful_stop_unavailable")}
        if managed:
            return {"success": True, "message": i18n.t("messages.stop_requested")}
        if wait_for_exit(pid, timeout=config.get("stop_timeout", 30)):
            return {"success": True, "message": "Stopped"}
    ok, error = force_kill(pid)
    return {"success": ok, "message": "Force killed" if ok else str(error)}
```

REST 게임은 예제의 콘솔 요청 대신 `daemon_rest(instance_id, method, path, body=...)`로 저장·종료를 요청한다. non-managed 프로세스 확인과 종료는 `is_process_alive`, `wait_for_exit`, `force_kill`을 쓴다. Windows에서 `os.kill(pid, 0)`은 프로세스를 끝내 버린다. 데몬이 넘긴 `pid`만 다루고 실행 파일 이름으로 프로세스를 찾아 죽이지 않는다.

### 사용자 값으로 경로 만들기

설정 이름이나 월드 이름을 경로에 붙일 때는 `safe_join`을 쓴다. 이름 조각마다 인자를 나눠 전달한다.

```python
from daemon_rcon import config_value, safe_join

world_dir = safe_join(config["working_dir"], config_value(config, "world_name", "world"))
```

`safe_join(root, *parts)`는 루트 안의 실제 경로를 `Path`로 돌려준다. 절대 경로, 드라이브, `..`, 구분자가 든 이름 조각과 루트 밖으로 나가는 링크는 `PathOutside`다. 넓은 `except Exception`이 있으면 `PathOutside`도 import하고 그 앞에 `except PathOutside: raise`를 둔다. `run_lifecycle`이 이 예외를 `module_path_outside`와 `message`·`path`로 바꾼다. 결과를 JSON으로 바꿀 수 없으면 `module_bad_output`, 다른 함수 예외는 `module_exception`이다.

서버 폴더 밖에 데이터를 두는 모듈은 데이터 루트를 선언한다. 경로 설정은 `type = "file"` 또는 `format = "path"`로 선언한다.

```toml
[extensions.saba-core]
data_roots = ["{data_dir}", "{home}/MyGame"]
```

`{home}`은 데몬 사용자의 홈 폴더다. 설정 자리는 사용자가 직접 고른 값이나 마이그레이션에서 읽은 path형 값만 허용 루트가 된다. 모듈 기본값만으로 외부 폴더를 열지 않는다. 데이터 루트는 `{home}` 또는 path형 설정 자리로 시작해야 한다. `..`나 다른 절대 경로는 선언할 수 없다. 백업·복원·설치·마이그레이션의 반환 경로도 데몬이 실제 경로로 검사한다. `verified` 모듈도 같은 검사를 받는다. 세부 규칙은 [모듈 계약 §2.1](https://github.com/WareAoba/saba-chan/blob/main/docs/architecture/02-modules.md#21-실행-방식-dsrcpluginmodrs266-463)을 따른다.

### 설정 화면: configure / import_settings

- 게임 설정 파일에 없는 값(실행 방식, 메모리, Java 경로, 실행 인자로만 쓰는 값)은 `group = "saba-chan"`으로 선언한다. 데몬이 이 값들을 `configure`에 넘기지 않으므로 게임 파일에 잘못 쓰일 일이 없다. 진입 파일은 config 최상위에서 읽는다.
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

- `risk`(`safe`/`caution`/`danger`)와 `description`은 AI 명령 해석이 읽는다. 서버 종료·추방·밴, 권한·밴 해제·허용 목록·계정·서버 옵션 변경, 아이템·돈·경험치·무적·게임 모드 지급은 `danger`. `caution`은 말하는 사바쨩이 확인 없이 실행할 수 있다. 무엇을 하는지 모르는 명령(plugin.yml이 선언한 명령 등)도 `danger`.
- 데몬이 `inputs`의 required·min·max·options를 실행 전에 검사하므로 `command`에서 다시 검사하지 않아도 된다.
- `command`에서 콘솔에 보내야 하면 `daemon_stdin_command(config["instance_id"], line)`.
- 예전 이름 `method = "rcon"` / `rcon_template`도 읽히지만 새 모듈은 `console` / `console_template`을 쓴다.

### 설치

- `[install] method = "download"`: 모듈의 `list_versions` / `install_server`가 내려받는다(minecraft).
  - `install_server`는 config `install_dir`에 `server_executable`(받을 실행 파일 이름, 요청이 없으면 `[config] server_executable`)로 둔다. `get_installed_version`은 시작·점검과 같은 config(`server_executable` 전체 경로, `working_dir`)를 받는다.
- `method = "steamcmd"` 같은 익스텐션 방식: 익스텐션이 설치를 맡고, 모듈은 `[install] requires_extensions`와 `[module.dependencies] ext-<id>`로 필요를 선언한다(zomboid, palworld).
- 익스텐션은 `import extensions.<이름>`으로 쓰되 함수 안에서 import하고, 없으면 죽지 말고 `error_code: "extension_required"`, `missing_extensions: [...]`로 답한다(palworld `ExtensionMissing`).
- 첫 설치에서 사용자가 고른 설정을 게임 파일에 쓰지 못하면 `success: false`, `error_code: "settings_write_failed"`, `message`로 답한다. 파일만 설치됐다고 성공으로 답하지 않는다.
- 배포물에 여러 OS 폴더가 있으면 OS 폴더 이름으로 고른다. 실행 파일 이름 일치, 경로 길이, 첫 항목으로 고르지 않는다. 고른 폴더가 없거나 둘 이상이면 실패로 답한다.
- 기존 설정·월드를 덮지 않는다. 동봉 예시 설정은 없을 때만 푼다.
- 받은 파일을 검증한다. 공식 체크섬이 있으면 받는 모든 바이너리에 쓴다. 공식 체크섬이 없으면 HTTPS와 고정된 공식 호스트, 압축 무결성(CRC), 기대 실행 파일의 존재로 확인한다. 조사 표에 "공식 체크섬 없음"과 근거 URL을 적는다. 체크섬을 지어내거나 커뮤니티 해시를 쓰지 않는다.
- 테스트는 실제 배포물과 같은 구조(모든 OS 폴더)의 가짜 압축 파일로 한다. OS마다 그 OS 파일만 풀리는지 본다.

### 실행 전제: 고정 판 포터블 런타임

서버가 배포물 밖에서 기대하는 런타임(Java, XNA Framework, VC++ 재배포 등)은 PC에 이미 설치돼 있어도 쓰지 않는다. 어디서나 같은 실행 환경을 지키기 위해서다.

1. 공식 재배포 파일의 주소, 크기, 서명, SHA-256을 조사 표에 적는다.
2. 서버가 실제로 쓰는 파일의 최소 집합을 실측으로 정한다. 테라리아는 XNA 4개(`Microsoft.Xna.Framework`, `.Game`, `.Graphics`, `.Xact`), `XnaNative.dll`, `msvcr100.dll`, `msvcp100.dll`이다.
3. `install_server`에서 고정 판을 받아 검증한다. 공식 체크섬이 없으면 서명을 확인한 파일의 SHA-256을 모듈에 고정한다.
4. 필요한 파일만 서버 폴더(또는 인스턴스 폴더)에 두고 그 경로로 실행한다.
5. `validate`는 그 파일이 없으면 `severity: "critical"`과 전용 `code`로 막고, 서버를 다시 설치하라고 안내한다.

관리자 권한, 시스템 설치, Windows 서비스를 쓰지 않는다. 한계도 적어 둔다. .NET Framework는 같은 이름·버전의 어셈블리가 GAC에 있으면 GAC 쪽을 먼저 읽는다. 네이티브 DLL은 서버 폴더의 것이 먼저 쓰인다.

### 격리 실행

공식 카탈로그에서 받지 않은 모듈은 "확인 안 됨"으로 Windows AppContainer 안에서 돈다. 새로 만든 모듈은 모두 이 상태로 시험한다. 격리 실행에서는 다음을 쓸 수 없다.

| 쓸 수 없는 것 | 대신 쓰는 것 |
|---|---|
| 루프백(데몬 API, 로컬 게임 서버 REST) | `daemon_stdin_command`, `daemon_rest`, `loopback_http`, `loopback_exchange` (데몬이 중계한다) |
| Windows Installer(`msiexec`, 오류 1601)와 다른 서비스 | 설치 파일 형식(MSI, CAB 등)을 Python으로 직접 읽는다 |
| `tempfile.mkdtemp`이 인스턴스 폴더에 만든 폴더 | `tempfile.mkstemp`, 또는 직접 만든 폴더. Python 3.13 이후 `mkdtemp`은 현재 사용자만 쓰는 권한을 붙여, 격리 계정이 그 안에 쓰지 못한다 |
| 허용되지 않은 폴더 쓰기 | 인스턴스 폴더, 작업 폴더, 설치 폴더, `data_roots`에 선언한 폴더 |

격리 때문에 공용 도우미가 실패하면 모듈에서 우회하지 않는다. 데몬·공용 도우미 문제로 보고한다.

### 콘솔 방식

데몬은 기본으로 서버 stdin·stdout을 파이프로 잇는다(`[launch] console = "pipe"`). 어떤 서버는 파이프에서 다르게 동작한다.

- 출력을 끝날 때까지 모아 두는 서버: 팰월드
- stdin이 파이프면 콘솔 입력을 읽지 않는 서버: 테라리아 Windows 서버. 명령과 `exit`에 답하지 않았고, 정지가 60초 뒤 강제 종료로 끝났다

이런 서버는 `[launch] console = "pty"`로 의사 콘솔(Windows ConPTY, unix pty)에서 띄운다. 의사 콘솔에서는 입력한 명령이 콘솔에 다시 찍히고, 줄 나눔이 달라질 수 있다. 테라리아는 프롬프트 `:`와 `Server started`가 다른 줄로 나왔다. 패턴은 두 경우를 모두 받게 쓴다.

### 콘솔 패턴

준비 완료(`[extensions.saba-core] ready_patterns`), 접속·퇴장(`player_join_patterns`, `player_leave_patterns`), 정상 종료(`[detection] graceful_exit_patterns`) 패턴은 실측한 원문으로만 쓴다.

- 줄 처음과 끝에 묶는다. 채팅에 같은 문구가 찍혀도 맞지 않게 한다(`<Server> Server started`).
- 줄 앞에 프롬프트가 붙을 수 있으면 `^(?:<프롬프트>)?`로 받는다.
- 서버 언어 설정이 문구를 번역하면 언어마다 실측한 문구를 모두 넣는다. 영어만 넣으면 다른 언어로 설정한 서버는 준비·정상 종료가 보이지 않는다.
- 테스트는 실측 원문 줄(접두 있음·없음)이 맞고 채팅 줄이 맞지 않는지 본다.

### 설정 파일의 형식

- 그 형식의 공식 문법으로 읽고 쓴다. 예: Java `.properties`에는 `\:`, `\=` 이스케이프와 줄 끝 `\` 이어짐이 있다.
- 인코딩을 가정하지 않는다. 실제 인코딩으로 읽고, 읽은 인코딩과 줄바꿈으로 다시 쓴다. 테라리아 Windows `serverconfig.txt`는 cp1252이고 주석에 0x92가 있다. UTF-8로만 읽으면 시작, 설정 저장, 가져오기가 모두 실패한다.
- 바뀐 키만 고치고 다른 키, 주석, 순서는 그대로 둔다.
- 파일 없음, 읽기 실패, 파싱 실패를 구별한다. 못 읽었으면 쓰지 않는다.
- 왕복 테스트를 쓴다. `configure`로 쓴 파일을 `import_settings`로 읽으면 같은 값이 나와야 한다. 특수 문자와 공식 파일의 원래 바이트를 테스트에 넣는다.

### 크래시 판정, 백업, 공지

- `[detection] graceful_exit_patterns`(정상 종료 문구)와 `crash_artifacts`(크래시 때 생기는 파일)를 적어야 데몬이 크래시만 골라 자동 재시작한다. 판정 순서는 module-spec.md `[detection]`에 있다.
- `[backup] paths`(작업 폴더 기준, `{설정 이름}` 치환), `save_command`(백업 직전 보낼 `[commands.fields]` 이름), 경로를 코드로 계산해야 하면 `hook = true` + `backup_paths(config)` → `{"success": true, "paths": [...], "restore_roots": [...]}`(`restore_roots`는 선택: 예전에 더 넓게 담은 백업도 복원되게 하는 범위이고 백업 대상은 아니다). 훅이 실패하면 백업도, 그 백업을 전제로 한 업데이트·복원도 멈춘다.
- `[config] announce_command`: 예약 재시작 예고·공지에 쓸 `[commands.fields]` 이름(`message` 입력 하나).

### 선택 기능: 조회, 접속자 수, 인스턴스별 명령, 약관, 프리셋, 권한

키의 정확한 규칙은 [모듈 계약](https://github.com/WareAoba/saba-chan/blob/main/docs/architecture/02-modules.md)에 있다. [module-spec.md](module-spec.md)는 저장소 안의 참조 안내다. 여기서는 언제 쓰는지만 적는다.

- **설정 필드 선택 키**: 포트는 `port = true`(여러 인스턴스를 만들 때 데몬이 겹치지 않게 옮긴다). 서버를 만들 때 골라야 하는 값(서버 종류처럼 받을 파일이 달라지는 값)은 `install = true` + `type = "select"`. 실행 인자로 들어가는 값은 `arg`, 값 형식은 `format`/`pattern`(데몬이 저장 때 검사하므로 모듈이 다시 검사하지 않아도 된다). `type`은 `text` `number` `password` `boolean` `select` `file`만 쓴다.
- **`[[queries]]`**: 접속자 목록처럼 읽기만 하는 조회. `function`이 `{"success": true, "data": ...}`를 돌려준다. 챗봇과 일반 사용자도 부르므로 아무것도 바꾸지 않는다.
- **`[status] players`**: 접속자 수 함수. `{"success": true, "online": 3, "max": 20}`. 데몬이 플레이어 목록을 물을 때는 config에 `players_detail: true`가 실리고, 그때만 `"players": [{"name": "Steve"}]`를 더한다(공식 방법으로 읽고, 문구가 다르면 짐작하지 말고 뺀다). 서버에 닿지 않으면 `success: false`, 서버는 정상인데 수를 모르면 `online` 없이 `success: true`. 서버 로컬 REST·소켓은 `daemon_rcon.loopback_http`/`loopback_exchange`로 연다(격리 실행에서도 데몬이 중계한다).
- **`[commands] hook = true` + `instance_commands`**: 설치된 플러그인처럼 서버 폴더에 따라 달라지는 명령. module.toml 명령 뒤에 붙는다.
- **`[[agreements]]` + `accept_agreements`**: 게임 EULA처럼 사용자가 직접 동의해야 하는 약관. 동의는 데몬이 이 PC에서만 받는다. 모듈은 `accepted_agreements`에 자기 id가 있을 때만 파일(예: eula.txt)에 쓰고, 동의를 가정하지 않는다.
- **`[[presets]]`**: 서버를 만들 때 고르는 추천 설정 묶음. 비밀번호·경로 설정은 넣지 못한다.
- **`[permissions]`**: 코드가 네트워크·프로세스 실행·네이티브 코드 등을 쓰면 선언한다. `saba-core module audit`과 적합성 키트가 선언하지 않은 사용을 찾는다.

## 5. 공용 도우미 (`daemon_rcon.py`, 데몬이 배포)

| 함수 | 용도 |
|---|---|
| `daemon_stdin_command(instance_id, line, *, timeout=10)` | 데몬이 관리하는 서버 콘솔에 한 줄 보내기 |
| `daemon_rest(instance_id, method, path, *, body=None, timeout=10)` | 데몬을 통해 게임 REST 요청 보내기 |
| `daemon_post(path, payload, timeout=10)` | 기존 HTTP 연동용 데몬 API 호출 |
| `is_process_alive(pid)`, `wait_for_exit(pid, timeout)`, `force_kill(pid, tree=True)` | PID 단위 프로세스 확인·대기·강제 종료 (Windows/POSIX 공용) |
| `config_value(config, key, default)` | config 세 위치에서 설정 값 찾기 |
| `load_setting_types(module_dir)`, `coerce_setting(value, type)` | 파일 값을 필드 타입으로 |
| `run_lifecycle(FUNCTIONS)` | `python lifecycle.py <함수>` 진입점 |
| `safe_join(root, *parts)`, `PathOutside` | 사용자 이름 조각을 루트 안 경로로 합치기·경로 위반 알리기 |

콘솔 명령은 `daemon_stdin_command`, REST는 `daemon_rest`를 쓴다. `SABA_HOST_DIR`이 있으면 SDK가 HOST 통로를 쓰고, 없으면 HTTP API로 보낸다. HOST의 대상은 데몬이 정한 현재 인스턴스다. 모듈이 다른 `instance_id`를 넘겨도 대상을 바꾸지 못한다. 인스턴스 없는 호출은 `no_instance`로 실패한다. 요청 한도는 실행당 64건, 건당 30초, 보내기 64 KiB, 받기 1 MiB다. `LOOPBACK`과 요청 수를 함께 센다. HOST 실패는 `error_code` 속성이 있는 `ConnectionError`다.

`daemon_rest`의 `path`는 게임 REST의 endpoint다. HTTP 호환 경로에서도 같은 endpoint로 전달된다. RCON만 받는 게임의 정지 명령은 기존 `daemon_post(f"/api/instance/{instance_id}/rcon", {"command": ...})`를 쓴다(§1).

SDK를 import하면 `module.toml`의 `[module.dependencies]`에 최소 코어 버전을 선언한다. `daemon_rcon.py`의 `SINCE`에서 쓰는 이름 중 가장 높은 버전을 고른다. 예제의 기존 함수는 `>=0.1.2`다. `safe_join`, `PathOutside`, `daemon_rest`를 쓰면 `>=0.1.10`으로 올린다. 범위·상한은 지원하지 않는다. `saba-core module audit`과 적합성 키트는 누락·낮은 선언을 `sdk_version`으로 알린다.

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

# 권한과 공용 SDK 최소 버전 선언 검사
saba-core module audit mygame

# 적합성 키트: 스키마, FUNCTIONS 표, 모르는 함수 답, 안전한 호출의 JSON 계약, 번역 키
pytest conformance --module path/to/mygame --shared-libs path/to/saba-chan/modules
```

키트는 데몬과 익스텐션 없이 함수를 부른다. 데몬에 닿지 않아도, 서버 파일이 없어도 모듈은 JSON으로 답해야 한다. 자세한 검사 목록은 [conformance/README.md](../conformance/README.md).

게임 동작은 모듈 폴더의 `test_lifecycle.py`(pytest)로 시험한다. 실제 게임 서버 없이 함수에 config를 넣고 답과 파일 변화를 본다(minecraft, palworld, zomboid의 `test_lifecycle.py` 참고). 테스트를 먼저 쓰고 실패하는 것을 확인한 뒤 구현한다.

단위 테스트와 적합성 검사는 격리 없이, 가짜 파일로 돈다. 그래서 실제 배포물, 격리 실행, 콘솔 방식, Windows에서만 생기는 문제를 보지 못한다. 아래 두 단계를 더 한다.

### 제작 환경 실측 (PR 전)

§0.3을 끝낸 상태에서 모듈 함수를 실제 서버 폴더에 직접 부른다.

1. `install_server`로 실제 배포물을 받아 푼다. 이 OS 파일만 풀렸는지 본다.
2. `get_launch_command`가 만든 명령으로 서버를 띄운다. 준비 패턴이 맞는지 본다.
3. 명령 템플릿을 보내 서버가 받아들이는지 본다.
4. 정지 명령을 보내 저장하고 끝나는지 본다.
5. `backup_paths`와 `reset_server`의 대상이 실제로 생긴 파일과 맞는지 본다.

단위 테스트만 통과한 상태를 "실측"이라고 쓰지 않는다.

### PC 실측

Windows PC의 정규 설치 앱에서 실제 서버로 확인한다. 새 모듈은 "확인 안 됨"으로 격리 실행되므로, 격리 실행에서 생기는 문제도 여기서 드러난다.

1. 모듈 폴더를 데몬 데이터 폴더의 `modules\<이름>\`에 두고 `saba-chan-cli module refresh`를 실행한다.
2. `saba-chan-cli server add <서버 이름> -m <모듈>`로 설치한다.
3. 서버를 켜고 준비 표시가 나오는지 본다.
4. 명령을 보내고(`saba-chan-cli run <서버> <명령>`), 콘솔(`saba-chan-cli server console <서버> --print`)에서 답을 본다.
5. 설정을 바꾸고 재시작해 서버가 그 값을 쓰는지 본다.
6. 백업을 만들고(`saba-chan-cli backup create <서버>`) 담긴 파일을 본다.
7. 서버를 끄고 정상 종료로 기록됐는지, 세이브가 남았는지 본다.
8. 월드 초기화 뒤 지운 파일과 남은 파일을 본다.

`[module.dependencies] saba-core`가 아직 발행되지 않은 버전이면 정식 설치는 그 모듈을 거절한다. 이때는 PC 실측을 그 버전이 발행된 뒤로 미루고 보고에 적는다. 의존 버전을 낮춰 우회하지 않는다.

PC 실측을 위해 설치된 데몬 파일을 바꿔 넣었다면, 바꾸기 직전에 실제로 설치돼 있던 파일을 백업하고 그 파일로 되돌린다. 그사이 자동 업데이트가 들어왔을 수 있다.

### Linux·macOS에서 개발 복사본 실행하기

확인되지 않은 모듈은 격리를 요구한다. Linux·macOS에는 Python 격리가 없으므로 현재 파일 지문에 동의하지 않으면 `action_required: "sandbox_unavailable"`, `error_code: "isolation_unsupported"`로 실행을 거부한다. Windows는 AppContainer를 쓰므로 격리 시험에 비격리 동의를 자동으로 넣지 않는다.

개발 복사본을 격리 없이 실행하려면 이 PC에서 코드를 확인한 뒤 다음 절차를 따른다.

1. 데몬이 모듈을 발견하도록 `mygame` 폴더를 배치한다.
2. 이 PC의 CLI에서 `saba-chan-cli sandbox allow module-mygame`을 실행하고 동의 질문에 답한다.
3. `saba-chan-cli sandbox list`로 동의가 현재 파일에 맞는지 확인한다.
4. 파일을 바꾸면 코드를 다시 확인하고 `sandbox allow module-mygame`을 다시 실행한다.
5. 동의를 철회하려면 `saba-chan-cli sandbox revoke module-mygame`을 실행한다.

데몬은 `config.json`의 `sandbox.unisolated`에 상대 경로와 sha256으로 만든 지문을 기록한다. `__pycache__` 안을 포함한 모든 실파일이 지문에 든다. Python 시험이 만든 캐시도 지문을 바꿀 수 있다. 데몬의 정상 Python 캐시는 구성요소 밖에 둔다. 설치·활성화만으로 동의가 생기지는 않는다.

격리 없이 import할 익스텐션도 확인된 상태이거나 자기 지문에 동의가 있어야 한다. 예를 들어 SteamCMD 개발 복사본은 `saba-chan-cli sandbox allow ext-steamcmd`로 따로 동의한다. 모듈 동의는 익스텐션을 승인하지 않는다. 이 동의는 Python 실행에 관한 것이며 게임 서버 프로세스는 격리하지 않는다.

## 8. 체크리스트

- [ ] `RESEARCH.md` 조사 표가 있고, 항목마다 출처와 "문서/실측" 구별이 있다 (§0.1)
- [ ] 배포물 목록과 동봉 기본 파일의 원래 바이트를 확인했다 (§0.2)
- [ ] 서버를 실제로 띄워 준비·정상 종료 원문, `help` 출력, 저장 뒤 생긴 파일을 기록했다 (§0.3)
- [ ] 선언한 명령이 `help` 출력과 한 줄씩 맞다
- [ ] 파이프로 보낸 명령에 서버가 답한다. 아니면 `[launch] console = "pty"`
- [ ] 콘솔 패턴이 실측 원문이고, 프롬프트 접두와 언어별 번역을 받는다
- [ ] 실행 전제가 고정 판 포터블이고, 받은 파일을 검증한다
- [ ] 설치가 이 OS 폴더만 푼다
- [ ] 설정 파일을 읽은 인코딩·줄바꿈 그대로 왕복한다
- [ ] 초기화가 회전 백업까지 포함한 서버 생성 파일만 지운다
- [ ] 격리 실행에서 쓸 수 없는 것(msiexec, 서비스, `mkdtemp` 폴더, 루프백 직접 연결)을 쓰지 않는다
- [ ] 제작 환경 실측과 PC 실측을 했거나, 못 한 항목을 보고에 적었다 (§7)
- [ ] 폴더 이름 = `[module] name`, `schema_version = 1`, `saba-core module validate` 통과
- [ ] `get_launch_command`가 진짜 서버 exe를 돌려준다(곧바로 끝나는 런처 exe가 아니라). 실행 파일이 없으면 `action_required: "executable_not_found"`
- [ ] `FUNCTIONS`에 만든 함수가 다 있고, 모든 실패가 종료 코드 0 + `success: false` + 이유
- [ ] `python lifecycle.py functions`가 FUNCTIONS 표의 이름을 돌려준다(`run_lifecycle`이면 자동)
- [ ] 로그는 stderr로만
- [ ] 콘솔·REST가 있으면 RCON은 끄고 `supported`에 `"rcon"`이 없다. RCON만 받는 게임이면 `supported = ["rcon"]`, `rcon_port`, 데몬이 준 `rcon_port`·`rcon_password`를 게임 설정에 쓰고 127.0.0.1에서만 받는다
- [ ] 콘솔이 있으면 `stop_command`를 선언했다. `managed: true`의 `stop`은 저장·종료 요청 뒤 바로 반환한다. non-managed만 데몬이 준 시간만큼 기다린 뒤 필요하면 강제 종료한다
- [ ] 정상 종료를 요청할 길이 없으면 죽이지 않고 `graceful_stop_unavailable`로 답한다. 프로세스는 데몬이 준 `pid`만 다룬다
- [ ] 사용자 이름 조각은 `safe_join`으로 경로에 붙인다. 넓은 예외 처리에서도 `PathOutside`를 다시 던진다
- [ ] 서버 폴더 밖 데이터는 `data_roots`에 선언했다. 첫 설치의 설정 기록 실패는 `settings_write_failed`로 답한다
- [ ] 명령은 `console` / `rest` 선언으로 되는 것은 선언으로, `risk`와 `description`을 적었다
- [ ] 크래시 흔적(`crash_artifacts`)과 정상 종료 문구(`graceful_exit_patterns`)를 적었다
- [ ] 익스텐션이 없을 때 `extension_required`로 답한다
- [ ] 훅(`[backup]`·`[commands]`)을 켰거나 `[[queries]]`·`[status]`가 가리킨 함수가 `FUNCTIONS`에 있다
- [ ] 코드가 쓰는 능력을 `[permissions]`에 선언했다(`saba-core module audit`)
- [ ] SDK import 중 가장 높은 `SINCE` 버전 이상을 `[module.dependencies] saba-core`에 선언했다
- [ ] `pytest conformance --module ...` 통과

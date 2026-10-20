# 사바쨩 모듈 제공 명세 (module.toml 스키마 v1)

모듈은 게임 서버 하나를 사바쨩 데몬이 다룰 수 있게 해 주는 "설명서"다. 폴더 하나에 아래 파일을 둔다.

| 파일 | 필수 | 내용 |
|---|---|---|
| `module.toml` | ✔ | 이 문서의 스키마 v1을 따르는 선언 |
| 진입 파일 (`[module] entry`, 보통 `lifecycle.py`) | ✔ | 데몬이 부르는 Python 함수들 (§3) |
| `icon.png` | ○ | `[module] icon`으로 지정 |
| `locales/<언어>.json` | ○ | 모듈 화면 문구 |

검사는 데몬과 같은 코드로 한다.

```
saba-core module validate <모듈 폴더>      # 실패하면 종료 코드 1
saba-core module schema                    # JSON Schema 출력 (= schema/module.schema.json)
```

`schema/module.schema.json`은 데몬의 파서 구조체에서 뽑은 것이다. 손으로 고치지 말고 `saba-core module schema > schema/module.schema.json`으로 다시 만든다.

## 1. 버전과 모르는 키

- `[module] schema_version = 1`을 쓴다.
- schema_version = 1인 파일에 모르는 키가 있거나 값이 규칙을 벗어나면 데몬은 **모듈을 로드하지 않는다**. 이유는 `GET /api/modules`의 `failed[]`에 나온다.
- schema_version이 없는 예전 모듈은 로드하되, 같은 문제를 각 모듈의 `warnings[]`에 남긴다.
- 데몬보다 새로운 schema_version은 거부한다("saba-chan을 업데이트하세요").

## 2. module.toml 키

✔ 필수, ○ 선택. 경로는 모두 모듈 폴더 또는 서버 작업 폴더 기준 상대 경로다.

### `[module]` ✔
| 키 | | 뜻 |
|---|---|---|
| `schema_version` | ✔ | `1` |
| `name` | ✔ | 모듈 id. 폴더 이름과 같게, 영문 소문자 |
| `version` | ✔ | semver |
| `entry` | ✔ | Python 진입 파일. 모듈 폴더 안의 경로여야 하고 파일이 있어야 한다 |
| `description`, `game_name`, `display_name`, `author`, `icon` | ○ | 표시용 |
| `log_pattern` | ○ | 콘솔 줄에서 레벨을 뽑는 정규식. `(?P<level>…)` 그룹 필수 |
| `[module.dependencies]` | ○ | 컴포넌트 → 최소 버전(`>=` 만). 키는 `versions.toml`과 같다: `saba-core`, `ext-<id>`, `gui`, `cli`. 예: `"saba-core" = ">=0.3.0"`. 데몬이 saba-core를 못 맞추면 모듈을 로드하지 않고, 나머지가 안 맞으면 `GET /api/modules`의 `unmet_dependencies`에 보인다 |

### `[protocols]` ○
`supported` (`"stdin"` `"rest"` 중. `"rcon"`은 보안 정책상 쓰지 않는다), `default`, `interaction_mode` (`"console"` 콘솔 화면 / `"commands"` 명령 버튼만).

### `[config]` ○
`server_executable`(작업 폴더 안 실행 파일 이름), `executable_path`, `process_name`, `default_port`, `stop_command`(stdin으로 보낼 정지 명령), `stop_timeout`(초, 기본 30), `announce_command`(게임 안 공지에 쓰는 `[[commands.fields]]` 이름, `message` 입력 하나. 예약 재시작·업데이트 예고와 공지 예약에 쓴다).

### `[launch]` ○
`log_files`: 서버가 콘솔 대신 파일에만 로그를 쓸 때, 그 파일들을 콘솔에 합친다. 와일드카드 `*` `?`는 마지막 칸에만.
`console`: `"pipe"`(기본) 또는 `"pty"`. stdout이 파이프면 끝날 때까지 모아 두는 서버(블록 버퍼링)는 `"pty"`로 의사 콘솔(Windows ConPTY)에 띄워 줄마다 받는다. stdout과 stderr가 한 갈래로 합쳐지고 터미널 제어 문자는 지워진다. stdin 명령은 Enter로 보내진다.

Windows에서 saba-runner는 서버를 Job Object에 넣는다. 러너가 끝나거나 강제 종료되면 서버와 서버가 띄운 프로세스가 같이 끝난다. 런처 exe(곧바로 끝나고 진짜 서버를 남기는 것)를 띄우면 러너가 남은 프로세스를 따라가지만 콘솔 출력은 받지 못하므로, `get_launch_command`는 진짜 서버 exe를 돌려준다.

### `[detection]` ○
| 키 | 뜻 |
|---|---|
| `process_patterns`, `cmd_patterns` | 이미 떠 있는 서버를 찾을 때 프로세스 이름·명령줄 패턴 |
| `dir_signatures` | 전부 있으면 이 게임의 서버 폴더로 본다 (`POST /api/modules/detect`) |
| `graceful_exit_patterns` | 정상 종료 때 콘솔에 찍히는 문구 (정규식). 사용자 정지와 크래시를 가르는 데 쓴다 |
| `crash_artifacts` | 크래시 때 생기는 파일 (작업 폴더 기준, 마지막 칸만 와일드카드). 서버 시작 뒤 새로 생기면 크래시로 본다 |

종료 판정 순서: 사바쨩이 보낸 정지 → 크래시 흔적(Windows 예외 종료 코드, unix 크래시 시그널, 새 `crash_artifacts` 파일) → `graceful_exit_patterns` → 종료 코드 0 → 원인 모름. 크래시만 자동으로 다시 띄운다(10분에 3번까지). 흔적을 선언하지 않으면 크래시를 알아보지 못하니, 게임이 크래시 때 남기는 파일을 꼭 적는다.

### `[install]` ○
`method` (`"manual"` 기본 / `"download"`는 내장, 그 밖의 값은 그 방식을 처리하는 익스텐션 id, 예: `"steamcmd"`), `app_id`, `anonymous`(기본 true), `install_subdir`, `platform`, `download_url`, `beta`(지정하면 항상 이 브랜치로 설치), `requires_extensions`(필요한 익스텐션 id 목록).
- 익스텐션 방식이면 데몬이 인스턴스 `extension_data`에 `install_method_<방식>`=true와 `<방식>_<필드>`(anonymous, app_id, beta, platform, download_url)를 쓴다. 서버 업데이트(`apply-update`)는 이런 인스턴스만 받는다.
- 모듈 Python은 `import extensions.<이름>`으로 익스텐션을 쓴다. 활성화된 익스텐션만 import된다: 데몬이 넣는 import 가드가 나머지를 `ModuleNotFoundError`로 막는다(`-`와 `_`는 같다). 익스텐션을 쓰는 코드는 함수 안에서 import하고 `ImportError`를 "익스텐션 없음" 응답으로 바꾼다(palworld의 `ExtensionMissing` 참고).

### `[[settings.fields]]` ○
인스턴스 설정 화면 항목. `name` ✔, `type`(`text` `number` `password` `boolean` `select` `file`, 기본 text. 이 밖의 값은 GUI가 그리지 않아 설정이 보이지 않는다), `label`, `description`, `required`, `default`, `min`, `max`, `step`, `options`(문자열 목록), `group`(`basic` 기본 / `advanced` / `saba-chan`). `saba-chan`은 게임 설정 파일에 없는 사바쨩 설정(실행 방식·Java 경로·메모리·실행 인자 등)이라는 선언이라, 설정을 저장할 때 `configure`의 `settings`로 넘어가지 않는다. 진입 파일은 이 값을 config 최상위에서 읽는다.
- `install = true`: 서버를 만들 때 고르는 값(받을 서버 파일이 달라지는 값, 예: 서버 종류). GUI 새 서버 화면과 CLI `server add`가 묻는다. GUI는 `select`만 보인다.
- `port = true`: 네트워크 포트 값. 같은 게임을 여러 개 만들면 데몬이 새 인스턴스의 포트를 모두 같은 폭(10의 배수)만큼 옮겨 다른 인스턴스와 겹치지 않게 한다. 인스턴스 필드 `port`·`rest_port`는 표시하지 않아도 옮겨진다. 사용자가 포트를 직접 주거나 기존 서버를 가져오면 옮기지 않는다.
- `arg`: 이 값이 실행 인자로 들어가는 모양. 문자열 하나나 배열이고, 공백으로 나눈 토큰에 `{value}`는 많아야 한 번이다(`"--port={value}"`, `"-servername {value}"`, `["-Xmx{value}", "-Xms{value}"]`). `{value}`가 없으면 boolean 플래그다. `{value}`뿐인 모양은 로드 오류다. 실행 인자 미리보기가 사용자가 친 추가 인자를 이 필드로 되돌릴 때만 쓴다. 실제 실행 줄은 여전히 `get_launch_command`가 만들므로 같은 모양으로 넣는다. 실행 인자로 들어가는 필드에만 쓴다.
- `format`: 값 형식. `memory_size`(숫자+K/M/G 한 글자, `4G`·`1024M`), `port`(1~65535), `integer`, `path`. `pattern`: 값 전체가 맞아야 하는 정규식. 둘 다 저장과 미리보기에서 검사하고, 틀리면 "잘못된 형식입니다"로 막는다(모듈이 안내 문구를 줄 필요는 없다).
- 문자열 `default`의 `{instance_dir}`은 새 인스턴스 폴더로 바뀐다(인스턴스마다 따로 둘 게임 데이터 폴더 등). 기존 서버를 가져올 때는 빈 값이 들어가므로 모듈은 빈 값을 그 게임의 기본 위치로 다룬다(zomboid `cache_dir` 참고).
데몬이 직접 의미를 아는 이름: `rest_port`, `rest_host`, `managed_start`, `log_sessions_keep`, `auto_restart`(크래시 때 다시 띄우기, 기본 true), `restart_on_unknown_exit`(원인 모를 종료도 다시 띄우기, 기본 false). 모듈이 선언하지 않아도 인스턴스 설정에 넣으면 데몬이 읽는다.

### `[[commands.fields]]` ○
명령 카탈로그에 실리는 게임 명령. GUI 버튼, CLI, 디스코드, AI 명령 해석이 모두 이 선언을 쓴다.

| 키 | | 뜻 |
|---|---|---|
| `name` | ✔ | 명령 이름 |
| `label`, `description` | ○ | 표시용. description은 AI가 명령을 고를 때도 읽는다 |
| `risk` | ○ | `"safe"`(조회·채팅) / `"caution"`(게임 상태 변경, 기본) / `"danger"`(서버 종료, 추방·차단, 권한 변경). AI가 추론한 명령을 실행 전에 확인받을지 정한다 |
| `method` | ○ | `"console"`(서버 콘솔 stdin) / `"rest"` / `"module"`(진입 파일의 `command()`, 기본). 예전 이름 `"rcon"`은 `"console"`로 읽는다 |
| `console_template` | console | 예: `"kick {player}"`. `{입력 이름}`이 치환된다. 예전 이름 `rcon_template` |
| `http_method`, `endpoint_template` | rest | REST 호출 |
| `inputs` | ○ | `{ name ✔, label, type, required, placeholder, default, min, max, options }` 목록. 데몬이 실행 전에 required·min·max·options를 검사한다 |

`[commands] hook = true`: 서버 폴더에 따라 달라지는 명령(설치된 플러그인 명령 등)을 진입 파일 `instance_commands(config)`가 인스턴스마다 더 준다. 돌려줄 것은 `{success, commands:[명령 필드와 같은 모양 + group?], components?}`. module.toml 명령과 이름·별명이 겹치거나 이름에 공백이 있는 것은 데몬이 뺀다.

### `[backup]` ○
백업(수동·예약·업데이트 전·복원 전)에 묶을 것. `paths`(작업 폴더 기준 폴더·파일, `{설정 이름}`은 인스턴스 설정 값으로 바뀐다), `save_command`(묶기 전에 실행할 `[[commands.fields]]` 이름, 예: `save-all`), `hook`(true면 진입 파일 `backup_paths(config)`가 `{"success": true, "paths": [...]}`로 경로를 더 준다. 실패하면 백업과 그 뒤 작업이 멈춘다). 없으면 수동 백업은 `backup_unsupported`, 업데이트 전·예약 백업은 건너뛴다.

### `[[agreements]]` ○
서버를 켜기 전에 사용자가 동의해야 하는 약관(예: 게임 EULA). `id`(소문자·숫자·`-`·`_`, 모듈 안에서 겹치지 않음), `url`(원문, https). 표시 이름은 모듈 locales의 `agreements.<id>`. 데몬이 동의를 받아 기록하고(이 PC에서만, 디스코드·챗봇에서는 받지 않는다), 동의하지 않은 약관이 있으면 시작을 막는다. 동의한 약관 id는 config `accepted_agreements`로 `install_server`·`accept_agreements`에 온다. 모듈이 직접 동의를 가정하면 안 된다.

### `[status]` ○
`players`: 접속자 수를 돌려주는 진입 파일 함수 이름(Python 식별자, 상태를 바꾸는 함수는 안 된다). 데몬이 켜진 서버마다 1분에 한 번, 10초 안에 부른다. 그 값은 서버 목록과 디스코드 `/사바쨩 상태`에 접속자 수로 나온다. 함수 계약은 §3 표.

### `[[queries]]` ○
읽기 전용 조회(예: 접속자 목록). 액션 `server.query:<name>`이 되어 GUI·CLI·디스코드·챗봇에서 부른다. `name`(소문자·숫자·`-`·`_`, 모듈 안에서 겹치지 않음), `label`, `description`, `function`(진입 파일의 함수 이름, Python 식별자), `inputs`(명령 입력과 같은 모양). 함수는 `{"success": true, "data": <JSON>}`을 돌려주고 아무것도 바꾸지 않는다(감사 기록 없이 챗봇·일반 사용자도 부른다). 표시 문구는 locales `queries.<name>.label`·`description`.
- 로드 때 검사: 입력 `type`은 `text`·`number`·`select`만, `select`에는 `options`가 있어야 하고 `min`은 `max` 이하, `function`에는 상태를 바꾸는 데몬 함수 이름(`stop`, `configure` 등)을 쓸 수 없다. 어기면 schema v1 모듈은 로드에 실패하고, `schema_version`이 없는 모듈은 경고를 남기고 조회를 모두 끈다.

### `[[presets]]` ○
서버를 만들 때 고르는 추천 설정 묶음. `id`(소문자·숫자·`-`·`_`, 겹치지 않음), `label`, `description`, `[presets.settings]`(`[[settings.fields]]` 이름 → 값). 값은 그 설정의 규칙(형식·범위·선택지)을 따라야 하고 `password`·`file`·`path` 형식 설정은 넣지 못한다. 어기면 로드 오류. 표시 문구는 locales `presets.<id>.label`·`description`.

### `[permissions]` ○
진입 파일 코드가 쓰는 위험한 능력의 선언. 모두 bool, 기본 false: `network`(데몬 API 호출은 들지 않음), `spawn_process`, `native_code`, `dynamic_code`, `env_secrets`, `fs_outside_instance`. 선언은 실행을 막거나 풀지 않는다(격리는 신뢰 표시로 정해진다). `saba-core module audit <모듈 폴더>`와 적합성 키트가 코드에서 찾은 능력 중 선언하지 않은 것을 알린다.

### 그 밖
- `[aliases]`: `module_aliases`(모듈 별명), `[aliases.commands.<명령>]` `aliases`·`description`.
- `[errors]`: 오류 코드 → 사용자 문구 (채팅 응답용).
- `[credential_map]`: 데몬 표준 키 → 게임 설정 키. 예: `rest_password = "AdminPassword"`.
- `[[syntax_highlight.rules]]`: `name`, `pattern`, `color`, `bold`, `italic`. 콘솔 강조.
- `[extensions.<익스텐션 id>]`: 익스텐션이 읽는 모듈별 설정. 데몬은 내용을 보지 않고 그대로 넘긴다.

## 3. 진입 파일 계약

데몬은 `python <entry> <함수 이름>`으로 실행하고 stdin에 JSON 하나를 쓴다. 모듈은 결과 JSON을 stdout에 쓰고 종료 코드 0으로 끝낸다. 결과는 stdout 전체이거나 **마지막 줄**이어야 한다(그 앞의 디버그 출력은 무시된다). 진행 상황은 stderr에 `PROGRESS:{json}` 줄로 쓴다.

- 데몬이 넣는 환경 변수: `SABA_IPC_URL`, `SABA_IPC_PORT`, `SABA_TOKEN_PATH`(토큰 파일 경로), `SABA_LANG`. 데몬 역호출은 `daemon_rcon.daemon_post()`를 쓴다.
- 공통 응답: `success: bool`, `message: str`. 사용자 조치가 필요하면 `action_required: "<코드>"`. 서버 실행 파일을 찾지 못하면(`get_launch_command`) 범용 코드 `executable_not_found`에 `configured_path`(설정된 경로, 있으면)와 `options: ["update_path", "install_new"]`를 싣는다. 파일 종류는 GUI·CLI가 `[config] server_executable`의 확장자로 정한다. 익스텐션이 없으면 `error_code: "extension_required"`, `missing_extensions[]`.
- 모르는 함수 이름에는 `{"success": false, "message": "Unknown function: X"}`.
- 예약 함수 `functions`: `{"success": true, "functions": [FUNCTIONS 표의 함수 이름…]}`. 데몬은 이것으로 모듈이 지원하는 기능을 알고, 없는 기능(예: `reset_server`가 없으면 서버 초기화)은 화면에서 숨긴다. 공용 `run_lifecycle(FUNCTIONS)`을 쓰면 자동으로 답한다. 진입점을 직접 쓰면 `FUNCTIONS["functions"] = lambda _config: {"success": True, "functions": [f for f in FUNCTIONS if f != "functions"]}` 한 줄을 더한다.

| 함수 | 언제 | 돌려줄 것 |
|---|---|---|
| `get_launch_command` ✔ | 서버 시작, 실행 인자 미리보기 | `program`, `args[]`, `working_dir`, `env_vars{}`. config에 `extra_args`(사용자가 더한 인자)가 온다: 알맞은 자리에 넣었으면 `extra_args_placed: true`(없으면 데몬이 끝에 붙인다). `preview: true`이면 미리보기다: 설정 파일·폴더를 쓰지 말고 실행 줄만 돌려준다 |
| `stop` | managed인데 stdin 정지 명령이 없거나 안 통할 때, 그리고 non-managed 정지 | `success` |
| `status` | non-managed 상태 조회 | `status`, `pid` |
| `command` | `method = "module"` 명령 | `success`, `message` |
| `get_credentials` | REST 접속 정보가 필요할 때 | `rest_username`, `rest_password` |
| `validate`, `configure`, `read_properties`, `reset_properties`, `reset_server` | 설정 화면 | `success` (+ 읽은 값) |
| `accept_agreements` | 사용자가 `[[agreements]]`에 동의한 뒤. config `accepted_agreements[]`에 있는 약관만 반영한다(예: eula.txt) | `success` |
| `diagnose_log` | 로그 진단 | `success` |
| `list_versions`, `get_version_details`, `install_server`, `get_installed_version` | 설치·업데이트 | 버전 목록 / `executable_path`, `install_path` / `version` |
| `import_settings` | 기존 서버 가져오기 | `settings{}` |
| `backup_paths` | 백업 때, `[backup] hook = true`이면 | `paths[]` (실패하면 백업과 그 뒤 업데이트·복원이 멈춘다) |
| `instance_commands` | 명령 목록·실행 때, `[commands] hook = true`이면 | `commands[]` |
| `[[queries]]`의 `function` | 조회할 때. config에 `args{입력 이름: 값}`이 온다 | `data` |
| `[status] players`가 가리키는 함수 | 켜진 서버마다 1분에 한 번 | `online`(0 이상 정수), `max`(정수, 모르면 빼거나 0). 서버에 닿지 않으면 `success: false`. 읽기만 한다 |

✔ 표시가 없는 함수는 기능을 지원할 때만 만든다. 만들었다면 반드시 `FUNCTIONS` 표에 넣는다. 훅을 켰거나(`[backup]`·`[commands]`) module.toml이 이름으로 가리키는 함수(`[[queries]]`, `[status] players`)는 반드시 만든다.

답을 준 실패(모르는 함수, 잘못된 설정, 익스텐션 없음)도 종료 코드 0으로 끝낸다. 데몬은 종료 코드가 0이 아니면 stdout을 버리고 일반 오류로 본다. 이 계약은 `conformance/`의 키트가 검사한다.

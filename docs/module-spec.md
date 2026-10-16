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
`supported` (`"rcon"` `"rest"` `"stdin"` 중), `default`, `interaction_mode` (`"console"` 콘솔 화면 / `"commands"` 명령 버튼만).

### `[config]` ○
`server_executable`(작업 폴더 안 실행 파일 이름), `executable_path`, `process_name`, `default_port`, `stop_command`(stdin으로 보낼 정지 명령), `stop_timeout`(초, 기본 30).

### `[launch]` ○
`log_files`: 서버가 콘솔 대신 파일에만 로그를 쓸 때, 그 파일들을 콘솔에 합친다. 와일드카드 `*` `?`는 마지막 칸에만.

### `[detection]` ○
| 키 | 뜻 |
|---|---|
| `process_patterns`, `cmd_patterns` | 이미 떠 있는 서버를 찾을 때 프로세스 이름·명령줄 패턴 |
| `dir_signatures` | 전부 있으면 이 게임의 서버 폴더로 본다 (`POST /api/modules/detect`) |
| `graceful_exit_patterns` | 정상 종료 때 콘솔에 찍히는 문구 (정규식). 사용자 정지와 크래시를 가르는 데 쓴다 |
| `crash_artifacts` | 크래시 때 생기는 파일 (작업 폴더 기준, 마지막 칸만 와일드카드). 서버 시작 뒤 새로 생기면 크래시로 본다 |

종료 판정 순서: 사바쨩이 보낸 정지 → 크래시 흔적(Windows 예외 종료 코드, unix 크래시 시그널, 새 `crash_artifacts` 파일) → `graceful_exit_patterns` → 종료 코드 0 → 원인 모름. 크래시만 자동으로 다시 띄운다(10분에 3번까지). 흔적을 선언하지 않으면 크래시를 알아보지 못하니, 게임이 크래시 때 남기는 파일을 꼭 적는다.

### `[install]` ○
`method` (`"manual"` 기본 / `"download"` / `"steamcmd"`), `app_id`, `anonymous`(기본 true), `install_subdir`, `platform`, `download_url`, `beta`(지정하면 항상 이 브랜치로 설치), `requires_extensions`(필요한 익스텐션 id 목록).

### `[[settings.fields]]` ○
인스턴스 설정 화면 항목. `name` ✔, `type`(`text` `number` `password` `boolean` `select` `file`, 기본 text), `label`, `description`, `required`, `default`, `min`, `max`, `step`, `options`(문자열 목록), `group`(`basic` 기본 / `advanced` / `saba-chan`).
데몬이 직접 의미를 아는 이름: `rcon_port`, `rest_port`, `rest_host`, `managed_start`, `log_sessions_keep`, `auto_restart`(크래시 때 다시 띄우기, 기본 true), `restart_on_unknown_exit`(원인 모를 종료도 다시 띄우기, 기본 false). 모듈이 선언하지 않아도 인스턴스 설정에 넣으면 데몬이 읽는다.

### `[[commands.fields]]` ○
명령 카탈로그에 실리는 게임 명령. GUI 버튼, CLI, 디스코드, AI 명령 해석이 모두 이 선언을 쓴다.

| 키 | | 뜻 |
|---|---|---|
| `name` | ✔ | 명령 이름 |
| `label`, `description` | ○ | 표시용. description은 AI가 명령을 고를 때도 읽는다 |
| `risk` | ○ | `"safe"`(조회·채팅) / `"caution"`(게임 상태 변경, 기본) / `"danger"`(서버 종료, 추방·차단, 권한 변경). AI가 추론한 명령을 실행 전에 확인받을지 정한다 |
| `method` | ○ | `"rcon"` / `"rest"` / `"module"`(진입 파일의 `command()`, 기본) |
| `rcon_template` | rcon | 예: `"kick {player}"`. `{입력 이름}`이 치환된다 |
| `http_method`, `endpoint_template` | rest | REST 호출 |
| `inputs` | ○ | `{ name ✔, label, type, required, placeholder, default, min, max, options }` 목록. 데몬이 실행 전에 required·min·max·options를 검사한다 |

### 그 밖
- `[aliases]`: `module_aliases`(모듈 별명), `[aliases.commands.<명령>]` `aliases`·`description`.
- `[errors]`: 오류 코드 → 사용자 문구 (채팅 응답용).
- `[credential_map]`: 데몬 표준 키 → 게임 설정 키. 예: `rest_password = "AdminPassword"`.
- `[[syntax_highlight.rules]]`: `name`, `pattern`, `color`, `bold`, `italic`. 콘솔 강조.
- `[extensions.<익스텐션 id>]`: 익스텐션이 읽는 모듈별 설정. 데몬은 내용을 보지 않고 그대로 넘긴다.
- `[docker]`: 도커 익스텐션 설정 (도커 익스텐션 유지 여부가 정해질 때까지 그대로 둔다).
- `[update] github_repo`: 예전 GitHub 업데이트 경로용. 데몬은 읽지 않고, 그 경로를 지울 때(L3) 같이 없앤다.

## 3. 진입 파일 계약

데몬은 `python <entry> <함수 이름>`으로 실행하고 stdin에 JSON 하나를 쓴다. 모듈은 결과 JSON을 stdout에 쓰고 종료 코드 0으로 끝낸다. 결과는 stdout 전체이거나 **마지막 줄**이어야 한다(그 앞의 디버그 출력은 무시된다). 진행 상황은 stderr에 `PROGRESS:{json}` 줄로 쓴다.

- 데몬이 넣는 환경 변수: `SABA_IPC_URL`, `SABA_IPC_PORT`, `SABA_TOKEN_PATH`(토큰 파일 경로), `SABA_LANG`. 데몬 역호출은 `daemon_rcon.daemon_post()`를 쓴다.
- 공통 응답: `success: bool`, `message: str`. 사용자 조치가 필요하면 `action_required: "<코드>"`. 익스텐션이 없으면 `error_code: "extension_required"`, `missing_extensions[]`.
- 모르는 함수 이름에는 `{"success": false, "message": "Unknown function: X"}`.

| 함수 | 언제 | 돌려줄 것 |
|---|---|---|
| `get_launch_command` ✔ | 서버 시작 | `program`, `args[]`, `working_dir`, `env_vars{}` |
| `stop` | managed인데 stdin 정지 명령이 없거나 안 통할 때, 그리고 non-managed 정지 | `success` |
| `status` | non-managed 상태 조회 | `status`, `pid` |
| `command` | `method = "module"` 명령 | `success`, `message` |
| `get_credentials` | RCON/REST 접속 정보가 필요할 때 | `rcon_port`, `rcon_password`, `rest_username`, `rest_password` |
| `validate`, `configure`, `read_properties`, `reset_properties`, `reset_server` | 설정 화면 | `success` (+ 읽은 값) |
| `accept_eula`, `diagnose_log` | EULA 동의, 로그 진단 | `success` |
| `list_versions`, `get_version_details`, `install_server`, `get_installed_version` | 설치·업데이트 | 버전 목록 / `executable_path`, `install_path` / `version` |
| `import_settings` | 기존 서버 가져오기 | `settings{}` |

✔ 표시가 없는 함수는 기능을 지원할 때만 만든다. 만들었다면 반드시 `FUNCTIONS` 표에 넣는다.

답을 준 실패(모르는 함수, 잘못된 설정, 익스텐션 없음)도 종료 코드 0으로 끝낸다. 데몬은 종료 코드가 0이 아니면 stdout을 버리고 일반 오류로 본다. 이 계약은 `conformance/`의 키트가 검사한다.

# module.toml 채우기 (선언으로 되는 것은 선언으로)

`new_module.py`가 `__example` 틀을 복사해 `[module] schema_version = 1`, `version = "0.1.0"`, `entry = "lifecycle.py"`를 둔다.
폴더 이름 = `[module] name`(영문 소문자). 쓰지 않는 기능의 함수는 만들지 않는다. 대신 조사 표의 모든 명령·설정이 어디에 들어갔는지는
빠짐없이 짝지어 둔다. 키 전체는 saba-chan `docs/architecture/02-modules.md` §1.2와 `schema/module.schema.json`이 기준이다.

## 통신 경로: STDIN → REST → RCON

- 게임 서버가 지원하는 경로 중 **stdin(콘솔)을 먼저**, 없으면 **REST**, 둘 다 없을 때만 **RCON**을 쓴다. 명령마다 같은 순서로 고른다(예: 콘솔에 없는 명령만 REST).
- `[protocols] supported`에 실제로 쓰는 것만 적는다. `interaction_mode`: `console` 또는 `commands`.
- 콘솔이나 REST가 있으면 RCON은 `supported`에 넣지 않고, 게임 설정의 RCON은 실행 전·설정 저장 때마다 끈다. 실행 중에 RCON을 다시 켤 수 있는 자유 입력 명령(lessons.md §안전·효율)도 막는다.
- **RCON만 받는 게임**(`supported = ["rcon"]`): `[protocols] rcon_port`(게임의 기본 RCON 포트)가 필수다. 데몬이 인스턴스마다 포트와 무작위 비밀번호를 정해 `rcon_port`·`rcon_password`로 넘긴다. 모듈은 `get_launch_command`(또는 실행 전 준비)에서 이 값을 게임 설정에 쓰고 RCON을 127.0.0.1에서만 받게 한다. 콘솔 명령과 정의에 없는 한 줄은 데몬이 RCON으로 보낸다. 모듈은 RCON 소켓을 직접 열지 않는다. 정지는 모듈 `stop()`이 `daemon_rcon_command(instance_id, "<정지 명령>")`로 보낸다 (saba-core 0.1.12+). RCON 포트·비밀번호는 설정 화면에 두지 않고 로그·응답·오류에 쓰지 않는다.

## 절마다

- `[config]`: `server_executable`, `default_port`, `stop_command`, `stop_timeout`(실측한 최대 저장 시간보다 넉넉히), `announce_command`.
- `[launch]`: 서버가 로그를 파일에만 쓰면 `log_files`, 콘솔이 tty를 요구하면 `console = "pty"`. 서버가 로그를 줄바꿈 없이 이어 써서 여러 줄이 한 줄로 붙으면 `line_start`에 실측한 줄 머리 정규식을 둔다. 줄 머리에 시각 같은 앞부분이 있으면 그것까지 넣는다(팰월드: `'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] \[LOG\]'`, `[LOG]`만 쓰면 시각이 앞 줄에 남는다). 빈 글자에 맞는 정규식은 거절된다. 머리 없이 붙는 문구는 나눌 수 없다.
- `[detection]`: `graceful_exit_patterns`, `crash_artifacts`, `dir_signatures`(기존 서버 가져오기), `cmd_patterns`. 준비 완료·접속·퇴장 패턴은 `[extensions.saba-core]`의 `ready_patterns`·`player_join_patterns`·`player_leave_patterns`·`chat_patterns`(게임 채팅, `name`·`message` 그룹)에 넣는다(마인크래프트 모듈과 같은 자리). 하나만 빠지면 그 게임만 접속자·준비가 안 보인다. 돌아가는 OS·CPU는 같은 표의 `platforms`(`windows`·`linux`·`macos`)·`architectures`(`x86_64`·`aarch64`, saba-chan 0.1.12)에 실측한 것만 적는다. 서버 실행 파일이나 설치 도구(SteamCMD는 32비트 x86)가 x86뿐이면 `architectures = ["x86_64"]`로 막는다(에뮬레이션은 하지 않는다).
- 콘솔 패턴은 `probe_server.py`로 얻은 원문으로만 쓴다. 줄 처음과 끝에 묶어 채팅에 같은 문구가 찍혀도 맞지 않게 한다. 줄 앞에 프롬프트가 붙을 수 있으면 `^(?:<프롬프트>)?`로 받는다. 언어 설정이 문구를 번역하면 언어마다 실측한 문구를 모두 넣는다. 영어만 넣으면 다른 언어로 설정한 서버는 준비·정상 종료가 안 보인다.
- `[install]`: `method`, `app_id`, `requires_extensions`; 익스텐션 방식이면 `[module.dependencies] ext-<id>`도. 같은 값을 lifecycle.py에 다시 적지 않는다.
- `[[settings.fields]]`: 조사한 설정 전부. `type`은 `text`·`number`·`password`·`boolean`·`select`·`file`만(그 밖은 GUI에 안 보인다). 범위는 `min`/`max`, 선택지는 `options`, 형식은 `format`/`pattern`, 기본값은 게임 기본 설정 파일의 값. 포트는 `port = true`, 실행 인자로 들어가는 값은 `arg`, 서버를 만들 때 고를 값은 `install = true`, 인스턴스별 폴더 기본값은 `{instance_dir}`, 게임 파일에 없는 실행 전용 값은 `group = "saba-chan"`. **경로 조각이 되는 이름**(서버 이름, 월드 이름)은 `pattern`으로 구분자·`..`를 막는다(코드 검증과 둘 다, lessons.md §사고). 데몬이 아는 이름(`rest_port`, `managed_start`, `auto_restart` …)은 02 §1.2 참고. 지금 타입으로 표현할 수 없는 설정(목록, 여러 줄)은 보고하고 범용 타입을 요청한다.
- `[[commands.fields]]`: 실제 서버의 `help`와 대조한 명령 전부. 문서에만 있고 서버가 모르는 명령은 넣지 않는다. 콘솔이면 `method = "console"` + `console_template`(공식 명령 이름 그대로), REST면 `method = "rest"` + `endpoint_template`, 여러 단계면 `module`(lifecycle `command`). 콘솔에서 필수인 인자는 `required = true`. 서버 폴더에 따라 달라지는 명령은 `[commands] hook = true` + `instance_commands`. 옵션 이름·값을 아무것이나 받는 setter 명령은 `select`로 제한한다.
- **위험도(`risk`)는 명령마다 스스로 판단해 반드시 적는다**(빠지면 caution이 된다). 판단 근거를 조사 표에 한 줄씩 남긴다.
  - `safe`: 읽기만 하거나 알림만 보낸다(목록, 정보, 공지, 저장).
  - `caution`: 플레이어나 월드에 영향을 주지만 되돌릴 수 있다(날씨·시간, 텔레포트, 화이트리스트 추가).
  - `danger`: 되돌리기 어렵거나 서버·데이터·권한에 크게 영향을 준다(정지, 추방, 밴, 관리자 권한 부여, 아이템·경험치 지급, 월드·플레이어 데이터 삭제, 서버 옵션 변경, 설정 다시 읽기, 대량 소환). 무엇을 하는지 모르는 명령도 `danger`.
  - 애매하면 높은 쪽을 고른다. `description`도 꼭 쓴다(AI·권한이 읽는다).
- 읽기 전용 조회는 `[[queries]]`(function 이름), 접속자 수는 `[status] players`(공식 API·명령으로).
- `[backup]`: lifecycle.md §백업.
- 약관은 `[[agreements]]`(id, https url) + `accept_agreements`. 모듈이 동의를 가정하지 않는다.
- 추천 설정 묶음은 `[[presets]]`. 비밀번호·경로 설정은 넣지 않는다.
- 코드가 쓰는 위험 능력은 `[permissions]`(network, spawn_process, native_code, dynamic_code, env_secrets, fs_outside_instance)에 선언한다. `saba-core module audit`이 맞는지 잰다.
- REST 비밀번호가 게임 설정 키에 있으면 `[credential_map]`.

## 콘솔 하이라이트 (`[[syntax_highlight.rules]]`)

틀의 예시 규칙(`WARN:`, `ERROR:`, `INFO:`, IP)은 그 게임의 콘솔과 상관없다. 규칙은 실제 서버 로그로 만든다. `check.py`의 highlight 관문이
틀 규칙만 남은 모듈을 막는다.

1. **로그를 모은다.** 실측(`trial.py`, PC 실측) 뒤 인스턴스의 저장 로그를 모은다: `saba-chan-cli server log <서버> --runs`로 실행 목록,
   `--source <실행> --all`로 실행 하나 전체, 또는 인스턴스 폴더의 `logs/sessions/*.log`. 시작·월드 생성·명령 전부·저장·정지·오류(설치 전제가
   빠졌을 때의 예외, 모르는 명령)가 들어간 실행을 고른다.
2. **줄 모양을 분류한다.** 앞의 `[시각] [STDOUT/INFO]` 머리를 떼고 숫자를 `N`으로 바꿔 모양마다 세면 수천 줄도 수십 가지로 줄어든다.
   많이 나오는 진행 줄(테라리아 월드 생성 `60.3% - Smoothing the world - 0.2%`는 한 번에 수만 줄)은 `dim`, 준비 완료는 `success`,
   정상 종료 문구는 `warn`, 예외·모르는 명령은 `error`, 채팅 보낸 이는 `player`, 프롬프트 뒤 다시 찍힌 명령은 `command`처럼 뜻으로 고른다.
3. **데몬 규칙을 지킨다.** 데몬 토큰화(saba-chan 02 §`[[syntax_highlight.rules]]`)는 Rust `regex`라 lookaround(`(?=`, `(?!`, `(?<=`, `(?<!`)가 없다.
   `color`는 토큰 16개(`error, warn, info, debug, success, string, number, keyword, timestamp, player, command, selector, ip, uuid, path, dim`)만 쓴다.
   일부만 칠하려면 `(?P<hl>…)`. 겹치면 `priority`가 큰 규칙이 이긴다. 채팅 줄(`<이름> 말`)을 높은 priority로 먼저 잡아 채팅 안 낱말이
   준비·오류 규칙에 걸리지 않게 한다.
4. **감지 패턴과 한 목록으로.** 준비·정상 종료 규칙의 문구 목록은 `ready_patterns`·`graceful_exit_patterns`와 같게 두고, 둘이 같은지 테스트로 맞춘다
   (언어를 더할 때 한쪽만 고치지 않게).
5. **테스트한다.** 모은 로그의 줄 원문을 (줄, 토큰, 칠할 글자) 표로 테스트에 넣는다(테라리아 `test_console_highlight_uses_measured_lines`).
   PC에서는 데몬의 `GET /api/instance/<id>/console?spans=1` 결과로 실제로 칠해지는지 본다.

# 선택 기능 (게임이 지원하면 넣는다)

필수 관문만 지나도 모듈은 돈다. 아래 기능은 게임이 그 길을 공식으로 주고 실측으로 확인했을 때 넣는다. 넣지 않은 기능은
이유(게임이 지원하지 않음, 실측 못 함 등)를 조사 표 "옮기지 못한 것과 이유"에 한 줄씩 적는다(관문 15). `check.py`의 optional 관문이
넣은 것과 안 넣은 것을 보고에 싣고, 데몬이 경고만 남기고 버리는 잘못된 선언(그룹이 빠진 채팅 패턴, 없는 함수를 가리키는 `chat_send`)을 막는다.
키의 정확한 규칙은 saba-chan `docs/architecture/02-modules.md` §1.2가 기준이다. 이 표는 무엇을 언제 넣는지만 말한다.

## 데몬 버전과 선택 키

- 0.1.11부터 데몬은 모르는 키를 경고만 남기고 무시한다. 그래서 더 새 데몬의 키를 넣어도 옛 데몬에서 모듈은 로드되고, 그 기능만 꺼진다.
  0.1.10 이하 데몬은 `schema_version`이 있는 모듈의 모르는 키에서 로드에 실패한다(`[extensions.saba-core]` 표 안의 키는 예외로 늘 경고만).
- 기능이 꼭 있어야 모듈이 맞게 도는 경우에만 `[module.dependencies] saba-core = ">=X.Y.Z-0"`을 올린다(`-0`이 있어야 베타 데몬도 받는다).
  버전 조건도 발행 때 한 번에 바꾼다.
- 스키마 2 설정 타입·키(`list`, `multiline`, `min_items`, `max_items`, `max_length`, `restart_required`, `visible_when`)는 다르다.
  `schema_version = 1` 모듈에 넣으면 로드가 실패한다. 쓰려면 `schema_version = 2`와 `saba-core = ">=0.1.9"`를 함께 둔다.

## 콘솔 줄로 하는 기능 (`[extensions.saba-core]`)

모든 패턴은 `probe_server.py`·PC 실측으로 얻은 줄 원문으로 쓴다. 줄 머리(시각·로그 레벨)를 줄 처음에 묶어, 채팅에 같은 글을 쳐도
맞지 않게 한다. 서버 언어 설정이 문구를 번역하면 언어마다 넣는다. 데몬은 콘솔을 읽는 시작(managed)에서만 이 패턴을 쓴다.

| 기능 | 넣는 것 | 게임에 필요한 것 | 지킬 것 |
|---|---|---|---|
| 준비 감지 | `ready_patterns`, 필요하면 `ready_timeout`(초, 기본 600, 최대 900) | 준비 완료 줄 | 준비 전에 들어온 정지는 이 줄 뒤에 보낸다. 없으면 시작 즉시 준비로 본다 |
| 접속·퇴장 | `player_join_patterns`·`player_leave_patterns` | 이름이 찍히는 접속·퇴장 줄 | 각 정규식에 `(?P<name>…)`. 둘 다 있으면 데몬이 이 줄로 접속자를 세고, 그 수가 `[status] players` 함수보다 앞선다. 이름 없이 id만 찍히면 넣지 않는다(좀보이드 B42) |
| 게임 채팅 → 디스코드 (0.1.12) | `chat_patterns` | 채팅 줄 | 각 정규식에 `(?P<name>…)`과 `(?P<message>…)`. 접속·퇴장 줄과 겹치지 않게 한다. 인스턴스 설정 `game_chat`(끔·전체·감지 단어)이 무엇을 보낼지 고른다 |
| 디스코드 → 게임 채팅 (0.1.12) | `chat_send = "<함수>"` + 그 함수 | 게임 안에 공지·채팅을 보내는 공식 명령·API | 함수는 config의 `chat_name`·`chat_text`(데몬이 한 줄로 다듬은 값)를 받아 게임에 알리고 `{"success": true}`. 서식 문자는 게임 문법대로 이스케이프하거나 뺀다(마인크래프트 `tellraw` JSON, `§` 서식 코드 제거). 예: 마인크래프트 `tellraw`, 테라리아 `say`, 팰월드 REST 공지 |
| 채팅 연동 (`/채팅연동`) | 위 세 줄(접속·퇴장, 채팅, 필요하면 `chat_send`) | | 따로 선언하지 않는다. 데몬이 접속자를 셀 수 있는 서버에서만 켜고, 센 접속자가 0명이면 끈다 |
| 진행 줄 접기 (0.1.11) | `progress_patterns` | 갱신마다 새 줄로 찍는 진행 표시 | 숫자만 다른 줄이 이어지면 단계마다 두 줄만 남긴다(테라리아 월드 생성) |
| OS·CPU 선언 | `platforms`(0.1.11)·`architectures`(0.1.12) | | 실측한 것만 적는다. x86 전용 실행 파일·SteamCMD면 `architectures = ["x86_64"]` |

## 명령과 접속자

| 기능 | 넣는 것 | 지킬 것 |
|---|---|---|
| 조작 가능한 사람만 접속 (0.1.13) | `[config] kick_command = "<명령 이름>"` | `[[commands.fields]]`의 추방 명령을 가리킨다. 그 명령의 첫 입력에 닉네임, 둘째 입력이 있으면 이유를 넣는다. 접속 줄(`player_join_patterns`)이 이름을 줘야 한다(적합성 키트가 잰다). 밴 명령을 가리키지 않는다 |
| 접속자 수 | `[status] players = "<함수>"` | 1분 갱신은 콘솔·RCON 명령이나 REST 요청을 보내지 않는다. config에 `players_detail: true`가 있을 때만(플레이어 목록 요청) 목록 명령을 보내 `players:[{name}]`를 더한다. 수를 알 수 없으면 실패 대신 `{"success": true}`(실패가 5분 이어지면 "응답 없음"으로 보인다) |
| 명령 답 문구 | `reply_pattern`(명령마다) + locales `commands.<이름>.done`, `[commands] failure_patterns` | 실측한 콘솔 답 원문으로 쓴다. 실패 답(권한 없음, 모르는 플레이어)을 `failure_patterns`로 잡으면 디스코드 답이 실패로 알린다. `(?P<reason>…)`이 있으면 그것이 이유다 |
| 디스코드에서 숨길 명령 | `remote = false` | 데몬 기능이 같은 일을 하는 명령. 게임에 직접 끄라고 보내는 명령(`stop`·`shutdown`·`quit`·`exit-nosave`)은 `danger` + `remote = false`다. 디스코드에서 끄는 길은 사바쨩 정지 하나다 |
| 조회 | `[[queries]]` + 함수 | 상태를 바꾸지 않는 함수만. 부른 사람만 보면 되는 결과(진단, IP)는 `private = true` |

## 백업

| 기능 | 넣는 것 | 지킬 것 |
|---|---|---|
| 켜진 서버 백업 확인 | `[backup] save_done_pattern` | 저장 명령이 다 썼다고 알리는 줄 원문. 있으면 데몬이 고정 3초 대신 그 줄을 최대 120초 기다리고, 안 나오면 백업을 만들지 않는다 |
| 자동 저장 멈춤 | `[backup] pause_command`·`resume_command` | 게임의 공식 쓰기 중지 절차(마인크래프트 `save-off` → `save-all flush` → 복사 → `save-on`). 둘 다 `[[commands.fields]]` 이름. `pause_command`만 두면 로드 실패 |
| 잠긴 파일 빼기 | `[backup] exclude` | 켜진 서버가 잠가 두는 파일 이름 패턴(`*` 가능) |
| 게임 자동 백업 가져오기 | `[backup] snapshots = true` + `backup_snapshots()` | 게임이 스스로 만든 백업을 `{snapshots: [{id, created, paths: [{from, to}]}]}`(새것부터)로 준다. zip이면 `archive`와 zip 안 경로. 사바쨩 백업에서는 여전히 그 폴더를 뺀다 |

## 설정 화면

| 기능 | 넣는 것 | 지킬 것 |
|---|---|---|
| 파일 키 한 곳 | `file_key` + 공용 `settings_to_file`·`settings_from_file` | 설정 이름 ↔ 게임 파일 키를 이 칸 하나에 둔다. 코드에 따로 표를 두지 않는다 |
| 포트포워딩 확인 | `public = "tcp"·"udp"·"both"` | `port = true`인 칸만. 친구가 밖에서 접속하는 포트에만 |
| 서버 종류 고르기 | `install = true` + `install_options` | 사바쨩이 설치할 수 있는 값만 `install_options`에 |
| 재시작 필요 표시 | `restart_required = true` (스키마 2) | 서버가 시작할 때만 읽는 값 |
| 조건부로 보이기 | `visible_when` (스키마 2) | 다른 설정 값에 따라 의미가 없는 칸 |
| 목록·여러 줄 | `type = "list"`·`"multiline"` (스키마 2) | 쉼표 목록을 한 줄 텍스트로 받지 않는다. 항목 수·길이는 `min_items`·`max_items`·`max_length` |
| 추천 설정 묶음 | `[[presets]]` | 비밀번호·경로 설정은 넣지 않는다 |

## 그 밖

| 기능 | 넣는 것 | 지킬 것 |
|---|---|---|
| 로그 진단 | `[[diagnose]]`(`code`, `severity`, `patterns`) + `diagnose_log` | 공용 `load_diagnose_rules`·`diagnose_lines`로 데몬이 준 `log_lines`에 맞춘다. 문구는 locales `errors.detect.<code>`·`errors.solutions.<code>` |
| 오류 문구 | `[errors]` 또는 locales `errors.<code>` | 명령 실패 응답에 붙는다 |
| 게임 이름 별명 | `[aliases] module_aliases` | 디스코드에서 게임 이름으로 서버를 부를 때 |
| 콘솔 레벨 | `[module] log_pattern` | `(?P<level>…)` 그룹이 필수 |

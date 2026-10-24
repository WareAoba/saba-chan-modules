# Terraria 모듈 조사 표

조사일 2026-10-24. 게임 정보는 아래 출처만 썼다. 출처가 없는 값은 비워 두고 "확인 못 함"에 적었다.

출처
- [W1] Terraria 공식 위키 Server: https://terraria.wiki.gg/wiki/Server
- [W2] Terraria 공식 위키 Guide:Setting up a Terraria server: https://terraria.wiki.gg/wiki/Guide:Setting_up_a_Terraria_server
- [T1] terraria.org 서버 목록 API: https://terraria.org/api/get/dedicated-servers-names (2026-10-24 답: `["terraria-server-1458.zip","terraria-server-1458.zip"]`)
- [T2] terraria.org 내려받기: `https://terraria.org/api/download/pc-dedicated-server/<파일 이름>` (W1이 안내하는 공식 zip)
- [N1] nixpkgs terraria-server 패키지(커뮤니티, zip 구조 근거): https://github.com/NixOS/nixpkgs/blob/master/pkgs/by-name/te/terraria-server/package.nix

## 서버

| 항목 | 값 | 근거 |
|---|---|---|
| 서버 실행 파일 | Windows `TerrariaServer.exe`, Linux `TerrariaServer.bin.x86_64` | W2(exe), N1(zip의 `Linux/TerrariaServer.bin.x86_64`) |
| 설치 | terraria.org 공식 zip. 최신 1.4.5.8 | W1, T1, T2 |
| zip 구조 | `<버전 숫자>/Linux/…`가 확인됨(N1). Windows 폴더 이름은 확인 못 함 → 모듈은 이름을 짐작하지 않고 zip 안에서 이 OS 실행 파일이 든 폴더를 찾아 그 폴더만 푼다 | N1 |
| 공식 체크섬 | 없음(terraria.org가 제공하지 않음). zip CRC 검사 + 실행 파일 존재로 확인, HTTPS·고정 호스트 | T1, T2 |
| 기본 포트 | TCP 7777 | W1, W2 |
| 명령 경로 | 콘솔(stdin). 게임에 REST·RCON 없음 | W1 |
| 정상 종료 | `exit` = 저장하고 종료, `exit-nosave` = 저장 없이 종료 | W1 |
| 자동 저장 | 매 새벽(24분마다) | W2 |
| 설정 파일 | `-config <파일>`로 읽는 `serverconfig.txt`, 한 줄에 `key=value`, `#`으로 시작하면 주석 | W1, W2 |
| Linux 실행 조건 | `TERM`이 xterm이 아니면 실패 → `TERM=xterm` | W2 |
| 월드 기본 위치 | Windows `Documents\My Games\Terraria\Worlds`, Linux `~/.local/share/Terraria/Worlds` | W1, W2 |
| 대화형 시작 | 설정 없이 켜면 월드 고르기 질문을 띄운다 → 모듈은 `world`·`autocreate`를 늘 쓴다 | W2 |

## 실행 인자 → 모듈

| 인자 | 어디로 | 근거 |
|---|---|---|
| `-config` | 모듈이 늘 `-config serverconfig.txt` | W1 |
| `-port`, `-players`/`-maxplayers`, `-pass`/`-password`, `-motd`, `-world`, `-autocreate`, `-worldname`, `-seed`, `-banlist`, `-secure` | 같은 값을 serverconfig.txt 키로 쓴다(비밀번호를 명령줄에 싣지 않는다, §5.1) | W1 |
| `-noupnp` | `upnp` 키로 | W1 |
| `-forcepriority` | `priority` 키로 | W1 |
| `-ip` | 설정 `ip` (사바쨩 설정, 인자) | W1 |
| `-steam` | 설정 `steam` | W1 |
| `-lobby friends|private` | 설정 `lobby` (`-steam`이 있을 때만 붙인다) | W1 |
| `-disableannouncementbox` | 설정 `disable_announcement_box` | W1 |
| `-announcementboxrange <n>` | 설정 `announcement_box_range` (-1 = 서버 전체) | W1 |

## serverconfig.txt 키 → 설정

| 키 | 설정 이름 | 형식·범위 | 기본값 | 근거 |
|---|---|---|---|---|
| `world` | (월드 폴더 + 월드 이름에서 계산) | 경로 | — | W1 |
| `worldname` | `world_name` | 이름(경로 조각, 구분자·끝 점/공백 금지) | `world` (사바쨩이 정한 이름, 게임 기본값 아님) | W1 |
| `worldpath` | `world_dir` | 경로 | `{instance_dir}/Worlds` (인스턴스 격리, §5.6) | W1 |
| `autocreate` | `autocreate` | 1 작음 / 2 중간 / 3 큼 | `2` (사바쨩 선택, 서버 생성 때 묻는다) | W1 |
| `seed` | `seed` | 문자열 | 없음 | W1 |
| `difficulty` | `difficulty` | 0 클래식 / 1 엑스퍼트 / 2 마스터 / 3 저니 | 없음(게임 기본) | W1 |
| `maxplayers` | `max_players` | 1~255 | 8 (공식 예시 설정 값) | W1, W2 |
| `port` | `port` | 포트 | 7777 | W1 |
| `password` | `password` | 문자열 | 없음 | W1 |
| `motd` | `motd` | 문자열 | 없음 | W1 |
| `banlist` | `banlist` | 서버 폴더 안 파일 이름 | `banlist.txt` | W1 |
| `secure` | `secure` | 1이면 켬 | 없음 | W1 |
| `language` | `language` | en-US, de-DE, it-IT, fr-FR, es-ES, ru-RU, zh-Hans, pt-BR, pl-PL | 없음 | W1 |
| `upnp` | `upnp` | 1이면 켬 | 없음 | W1 |
| `npcstream` | `npcstream` | 0 이상, 0 = 끔 | 없음(예시 60) | W1 |
| `priority` | `priority` | 0 실시간 ~ 5 유휴 | 없음(예시 1) | W1 |
| `journeypermission_*` 15개 | 같은 이름 | 0 잠금 / 1 호스트만 / 2 모두 | 없음 | W1 |

게임은 기본 설정 파일을 만들지 않으므로 기본값이 공식으로 정해진 키(포트·밴 목록)만 기본값을 두었다. 나머지는 사용자가 정할 때만 파일에 쓰고, 값을 비우면 그 줄을 지워 게임 기본값으로 돌아간다.

## 명령 → module.toml (모두 `method = "console"`, 이름은 W1 그대로)

| 명령 | risk | 판단 근거 |
|---|---|---|
| `help`, `playing`, `version`, `time`, `port`, `maxplayers`, `motd`(보기) | safe | 읽기만 |
| `death`, `pvpdeath`, `alldeath`, `allpvpdeath` | safe | 읽기만 |
| `clear` | safe (`remote = false`) | 서버 콘솔만 지운다 |
| `save` | safe | 저장 |
| `say <message>` | safe | 공지 (`announce_command`) |
| `password`(보기) | caution (`remote = false`) | 비밀번호를 콘솔에 찍는다 |
| `dawn`, `noon`, `dusk`, `midnight` | caution | 월드 시간 변경, 되돌릴 수 있음 |
| `settle` | caution | 월드 물 상태 변경 |
| `kick <player>` | danger | 추방 |
| `ban <player>` | danger | 밴(밴 목록 파일에 기록) |
| `exit-nosave` | danger | 저장 없이 종료 |
| `exit` | — | 데몬 정지(`stop_command`)가 쓴다 |

## 옮기지 못한 것과 이유

- `password <pass>`, `motd <message>`(바꾸기): 실행 중 값만 바뀌고 serverconfig.txt는 그대로라 단일 설정원(§8.3)을 깬다. 설정 화면의 `password`·`motd`로 바꾸고 다시 켠다. 실행 중 적용이 필요하면 데몬에 "설정 저장 때 켜진 서버에 반영할 명령" 같은 범용 지점이 먼저 필요하다.
- 밴 해제: 게임에 명령이 없다(W1). 밴 목록 파일을 고치는 모듈 명령은 공식 기능이 아니라 넣지 않았다.
- `[status] players`(접속자 수): `playing` 출력 형식의 공식 근거가 없다. 실측 뒤 추가.
- `worldrollbackstokeep`: 위키에 없다. 실측(서버 파일) 뒤 추가.

## 확인 못 함 (Windows PC 실측 필요)

1. Windows zip 안 폴더 이름과 동봉 `serverconfig.txt` 유무·내용, 서버가 쓰는 파일 인코딩(UTF-8 가정).
2. 데몬이 stdin을 파이프로 줄 때 서버가 명령을 읽는지(안 되면 `[launch] console = "pty"`).
3. 콘솔 문구: 준비 완료·접속·퇴장·정상 종료·크래시 파일(`crashlog.txt` 등). 근거가 없어 `ready_patterns`·`player_join/leave_patterns`·`graceful_exit_patterns`·`crash_artifacts`를 비웠다.
4. serverconfig 문법 세부: 키 대소문자, 값 앞뒤 공백, 같은 키 두 번일 때 어느 줄을 쓰는지, 값 안의 `=`·`#`. 모듈은 첫 `=` 뒤 전부를 값으로 보고, 같은 키는 한 줄만 남긴다.
5. `.wld.bak` 등 게임이 만드는 월드 백업 파일 이름(초기화가 지우고 백업이 빼는 대상).
6. `exit` 저장 시간(지금 `stop_timeout = 60`), SIGTERM이 저장하는지(모르므로 쓰지 않는다).
7. 1.4.5에서 Windows 서버에 따로 필요한 런타임이 있는지.

## 다른 모듈과 방식이 다른 곳

- `get_launch_command`가 시작 전에 serverconfig.txt의 `world`·`worldpath`·`worldname`·`autocreate`·`port`를 인스턴스 값으로 맞춘다(minecraft가 실행 전 RCON 정책을 쓰는 것과 같은 자리). 대화형 질문으로 멈추지 않게 하고 월드를 인스턴스 폴더에 묶기 위해서다.
- 가져온 서버에 serverconfig.txt가 없으면 월드 폴더를 공용 Documents가 아니라 인스턴스 폴더로 둔다(§5.6 격리). 기존 월드를 쓰려면 설정에서 월드 폴더를 고른다(`data_roots = ["{world_dir}"]`).
- RCON 정책: 게임에 RCON이 없어 끌 것이 없다.

## §8 점검

| 항목 | 결과 |
|---|---|
| 빈 이름이 폴더 전체를 지움(modules #51) | `config_value` + 기본 이름, 이름 검사는 `_world()` 한 곳, `safe_join`, 지우는 대상이 월드 폴더 바로 아래인지 확인 |
| 허용 목록 밖 전부 삭제(modules #52) | 정해진 두 파일(`<이름>.wld`, `<이름>.wld.bak`)만 지움. 실행 파일이 없는 폴더면 거절 |
| 명령 이름 짐작 | 모두 W1 목록 그대로 |
| 콘솔에서 필수인 인자 | `kick`·`ban`·`say`의 인자 `required = true` |
| 백업 "저장 + 몇 초" | `save_command = "save"`. 공식 쓰기 중지 명령이 없다(W1) |
| 콘솔 출력 파싱 | 하지 않음(근거 없음) |
| 공식 실행 정보 재해석 | 해당 없음(실행 파일 직접) |
| 인자 표기 | W1 표기 그대로(`-ip`, `-lobby` …) |
| 강제 정지 공식 API | 없음. 데몬 PID만 `force_kill` |
| 버전·적용 설정 API | 없음 |
| 체크섬 | 공식 없음(위 표) |
| 불리언 판정 | `1`/`0`만 불리언으로 읽음 |
| 기본값 세 곳 | 코드 상수는 toml 기본값과 같음(포트 7777, banlist.txt, world, 2) |
| 읽기 실패를 빈 설정으로 | 못 읽으면 쓰지 않고 실패(테스트) |
| 설정 사본으로 월드 위치 | 가져올 때 `world=` 경로에서 폴더·이름을 읽음 |
| 데몬 `log_lines` 무시 | `diagnose_log` 없음 |
| 아무 옵션 setter | 없음(위 "옮기지 못한 것") |
| 비밀번호 명령줄 | 파일에만 씀 |
| 실행 중 인증·포트 변경 | 관리 경로가 콘솔이라 해당 없음 |
| 저장 명령 + sleep 후 종료 | `exit`만 보냄(저장 포함) |
| 공용 홈 폴더로 되돌아가기 | 월드 폴더 기본값이 인스턴스 폴더 |

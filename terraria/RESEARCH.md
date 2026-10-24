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
| zip 구조 | 1.4.5.8 공식 zip에서 `1458/Linux/`(33개, Mono용 `TerrariaServer.exe` 포함), `1458/Windows/`(`TerrariaServer.exe`, `ReLogic.Native.dll`, `serverconfig.txt`, `start-server.bat`, `changelog.txt` 포함), `1458/Mac/Terraria Server.app/Contents/Resources/TerrariaServer.exe`를 확인했다. 설치는 OS 폴더 바로 아래의 해당 OS 실행 파일만 있는 폴더를 고른다 | 2026-10-24 실측 |
| 공식 체크섬 | 없음(terraria.org가 제공하지 않음). zip CRC 검사 + 실행 파일 존재로 확인, HTTPS·고정 호스트 | T1, T2 |
| 기본 포트 | TCP 7777 | W1, W2 |
| 명령 경로 | 콘솔(stdin). 게임에 REST·RCON 없음 | W1 |
| 정상 종료 | `exit` = 저장하고 종료, `exit-nosave` = 저장 없이 종료 | W1 |
| 자동 저장 | 매 새벽(24분마다) | W2 |
| 설정 파일 | Windows 폴더에 예시 `serverconfig.txt`가 있다. Windows-1252, CRLF이며 주석에 바이트 `0x92`가 들어간다. `-config <파일>`로 읽고 한 줄에 `key=value`, `#`으로 시작하면 주석 | 2026-10-24 실측, W1, W2 |
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
| `language` | `language` | en-US, de-DE, it-IT, fr-FR, es-ES, ru-RU, zh-Hans, pt-BR, pl-PL, ja-JP, ko-KR, zh-Hant | 없음 | W1, Windows serverconfig 예시, 2026-10-24 실측 |
| `upnp` | `upnp` | 1이면 켬 | 없음 | W1 |
| `npcstream` | `npcstream` | 0 이상, 0 = 끔 | 없음(예시 60) | W1 |
| `priority` | `priority` | 0 실시간 ~ 5 유휴 | `1`(Windows 예시 파일의 유일한 비주석 줄) | 2026-10-24 실측 |
| `seed_celebration`, `seed_theconstant`, `seed_notthebees`, `seed_notraps`, `seed_fortheworthy`, `seed_remix`, `seed_drunk`, `seed_zenith` | 같은 이름 | 불리언, 0 끔 / 1 켬 | 없음 | Windows 예시 파일, 2026-10-24 실측 |
| `worldrollbackstokeep` | 같은 이름 | 0 이상의 정수 | 서버 기본값 | Windows 예시 파일, 2026-10-24 실측. 값을 주지 않으면 `.bak`와 `.bak2`, 0이면 `.bak` 생성 |
| `slowliquids` | 같은 이름 | 불리언, 0 끔 / 1 켬 | 없음 | Windows 예시 파일, 2026-10-24 실측 |
| `language` 추가 선택지 | `language` | `ja-JP`, `ko-KR`, `zh-Hant` | 없음 | 공식 파일 목록, 2026-10-24 서버 적용 실측 |
| `journeypermission_*` 15개 | 같은 이름 | 0 잠금 / 1 호스트만 / 2 모두 | 없음 | W1 |

예시 설정의 주석에는 `#motd=Please don’t cut the purple trees!`가 있으며 Windows-1252의 `0x92` 바이트를 쓴다. 설정을 바꿀 때 읽은 인코딩과 줄바꿈을 유지한다. 새 설정 파일은 UTF-8로 쓴다.

## 명령 → module.toml (모두 `method = "console"`, 1.4.5.8 `help` 실측)

| 명령 | risk | 판단 근거 |
|---|---|---|
| `help`, `playing`, `version`, `time`, `port`, `maxplayers`, `motd`(보기) | safe | 읽기만 |
| `seed` | safe | 현재 월드 시드 표시, 읽기만 |
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

실측한 전체 목록은 `help`, `playing`, `clear`, `exit`, `exit-nosave`, `save`, `kick <player>`, `ban <player>`, `password`, `password <pass>`, `version`, `time`, `port`, `maxplayers`, `say <words>`, `motd`, `motd <words>`, `dawn`, `noon`, `dusk`, `midnight`, `settle`, `seed`다. 언어를 바꿔도 영문 명령은 그대로 동작한다(de-DE, ru-RU 실측).

## 옮기지 못한 것과 이유

- `password <pass>`, `motd <message>`(바꾸기): 실행 중 값만 바뀌고 serverconfig.txt는 그대로라 단일 설정원(§8.3)을 깬다. 설정 화면의 `password`·`motd`로 바꾸고 다시 켠다. 실행 중 적용이 필요하면 데몬에 "설정 저장 때 켜진 서버에 반영할 명령" 같은 범용 지점이 먼저 필요하다.
- 밴 해제: 게임에 명령이 없다(W1). 밴 목록 파일을 고치는 모듈 명령은 공식 기능이 아니라 넣지 않았다.
- `[status] players`(접속자 수): 접속자 수 표시는 실측하지 못했다.

## 2026-10-24 실측 (공식 1.4.5.8 zip)

공식 zip을 WSL에서 stdin 파이프로 실행하고 Windows 서버도 실행했다.

- 명령은 표의 `help` 출력과 같다. `death`, `pvpdeath`, `alldeath`, `allpvpdeath`는 `Invalid command.`를 반환했다.
- 준비 완료와 종료 문구는 언어별로 달라졌다. `module.toml`에는 아래 12개 언어의 문구를 줄 전체 일치 패턴으로 등록했다. 프롬프트 `: ` 접두사는 붙을 수도, 붙지 않을 수도 있다.

| 언어 | 준비 완료 | 종료 후 저장 |
|---|---|---|
| en-US | `Server started` | `Saving before exit...` |
| de-DE | `Server gestartet` | `Vor dem Verlassen speichern ...` |
| it-IT | `Server avviato` | `Salvataggio prima dell'uscita...` |
| fr-FR | `Serveur démarré` | `Enregistrement avant de quitter…` |
| es-ES | `Servidor iniciado` | `Guardando antes de salir...` |
| ru-RU | `Сервер запущен` | `Сохранение перед выходом...` |
| zh-Hans | `服务器已启动` | `正在进行退出前的保存……` |
| pt-BR | `Servidor inicializado.` | `Salvando antes de sair...` |
| pl-PL | `Serwer uruchomiony` | `Zapisywanie przed wyjściem...` |
| ja-JP | `サーバーが開始しました` | `セーブ中…` |
| ko-KR | `서버 시작` | `종료 전에 저장하는 중...` |
| zh-Hant | `伺服器已啟動` | `離開前儲存中……` |
- `exit` 저장 때 `<월드>.wld.bak`, `<월드>.wld.bak2`가 만들어졌다. `worldrollbackstokeep=0`이면 `.bak`만 남았다.
- Windows PC에 XNA Framework 4.0이 없으면 `TerrariaServer.exe`가 `System.IO.FileNotFoundException`으로 종료됐다. 참조 어셈블리는 `Microsoft.Xna.Framework` 4.0.0.0 및 `Graphics`, `Game`, `Xact`이며 zip에는 포함되지 않는다. 모듈은 설치를 대신하지 않고 필요한 런타임을 안내한다.
- 서버는 인스턴스 바깥의 `~/.local/share/Terraria/favorites.json`에 쓴다. Windows에서는 `Documents\\My Games\\Terraria` 아래다. 현재 모듈은 이 파일을 격리하지 않는다.

## 확인 못 함

- 접속·퇴장 문구와 접속자 수는 클라이언트 없이 실측하지 못했다. 패턴을 등록하지 않았다.
- `kick`·`ban`의 실제 대상 처리는 실측하지 못했다.
- serverconfig의 키 대소문자, 값 앞뒤 공백, 중복 키와 값 안의 `=`·`#` 동작은 확인하지 않았다.
- `exit` 저장 시간과 SIGTERM 저장 여부는 확인하지 않았다.

## 다른 모듈과 방식이 다른 곳

- `get_launch_command`가 시작 전에 serverconfig.txt의 `world`·`worldpath`·`worldname`·`autocreate`·`port`를 인스턴스 값으로 맞춘다(minecraft가 실행 전 RCON 정책을 쓰는 것과 같은 자리). 대화형 질문으로 멈추지 않게 하고 월드를 인스턴스 폴더에 묶기 위해서다.
- 가져온 서버에 serverconfig.txt가 없으면 월드 폴더를 공용 Documents가 아니라 인스턴스 폴더로 둔다(§5.6 격리). 기존 월드를 쓰려면 설정에서 월드 폴더를 고른다(`data_roots = ["{world_dir}"]`).
- RCON 정책: 게임에 RCON이 없어 끌 것이 없다.

## §8 점검

| 항목 | 결과 |
|---|---|
| 빈 이름이 폴더 전체를 지움(modules #51) | `config_value` + 기본 이름, 이름 검사는 `_world()` 한 곳, `safe_join`, 지우는 대상이 월드 폴더 바로 아래인지 확인 |
| 허용 목록 밖 전부 삭제(modules #52) | 월드 하나의 `.wld`, `.wld.bak`, `.wld.bak<숫자>`만 지움. 실행 파일이 없는 폴더면 거절 |
| 명령 이름 짐작 | 실측 `help` 명령을 사용하고 없는 death 명령을 제거 |
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

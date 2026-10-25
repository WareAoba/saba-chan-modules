---
name: saba-module-maker
description: 사바쨩(saba-chan)에 새 게임 서버 모듈을 만들거나 기존 모듈에 기능을 더하거나 고칠 때 쓴다. 사용자 설계 원칙과 PR 전 관문(실물 확인, 실제 서버 실행, 포터블 런타임, 격리 실행)을 지키고, 공식 자료와 실측으로 module.toml·lifecycle.py·locales를 계약대로 쓴 뒤 도구로 검사와 실측까지 마친다.
---

# 사바쨩 모듈 만들기

모듈은 게임 서버 하나를 데몬이 다루게 하는 "설명서"다: `saba-chan-modules/<game>/{module.toml, lifecycle.py, locales/, icon.png}`.
데몬이 `python lifecycle.py <함수>`로 한 번씩 실행한다(stdin JSON → stdout JSON). 새 모듈뿐 아니라 기존 모듈에 기능을 더하거나
고칠 때도 이 문서를 따른다.

이 문서는 어느 에이전트(Claude, Codex, GPT 등)든 그대로 따를 수 있게 썼다. 경로는 모듈 저장소(saba-chan-modules) 루트 기준이다.
먼저 아래 세 절(설계 원칙, PR 전 관문, 작업 순서)을 읽는다. 단계마다 가리키는 `references/` 문서를 그때 연다.

## 사용자 설계 원칙 (협상하지 않는다)

사용자가 정한 원칙이다. 다른 판단보다 앞선다.

1. **데몬이 모든 기능의 주체.** 모듈은 데몬이 부르는 설명서다. 데몬 계약을 모듈 안의 꾀로 우회하지 않는다. 모자라면 데몬에 범용 지점을 먼저 만든다.
2. **게임 이름으로 분기하지 않는다.** 데몬·공용 도우미에 특정 게임 이름을 넣지 않고 manifest·hook·slot 같은 범용 지점을 쓴다.
3. **어디서나 같은 실행 환경.** 서버를 돌리는 런타임(Java, XNA, VC++ 등)은 PC에 이미 설치돼 있어도 쓰지 않는다. 버전을 고정한 공식 파일을 받아 서버(인스턴스) 폴더에 두고 그 경로로 실행한다. 관리자 권한·시스템 설치·서비스를 쓰지 않는다.
4. **값은 한 곳에.** 설정은 설정 파일 하나, 구성요소 선언은 manifest 하나, 기본값은 게임이 만든(또는 동봉한) 기본 파일 하나가 원본이다.
5. **사용자 문구는 locales로.** 한국어 기준, 10개 언어.
6. **버전은 발행 때 한 번만** 올린다. 초안·PR에서 올리지 않는다.

## PR 전 관문

하나라도 못 지나면 PR을 열지 않거나, 못 지난 항목을 PR 첫머리에 적는다. 각 관문은 실행한 결과로만 통과로 적는다.
Windows PC가 없는 환경(클라우드 Linux 등)에서 잴 수 없는 관문은 "PC 실측 필요"로 남긴다.
"도구" 칸은 그 관문을 재는 이 묶음의 도구다. `check.py`가 기계로 잴 수 있는 관문을 한 번에 돌리고, 나머지는 "직접 확인" 목록으로 보여 준다.

| # | 관문 | 도구 | 자세히 |
|---|---|---|---|
| 1 | **조사 표**가 있다. 모든 명령·설정·포트·경로·콘솔 문구에 출처가 붙고 "문서"와 "실측"이 구별돼 있다 | `new_module.py`가 만든 `RESEARCH.md`, `check.py` research | [research.md](references/research.md) |
| 2 | **실물을 열어 봤다.** 배포물 항목 전체 목록, 동봉 기본 파일의 원래 바이트(인코딩·줄바꿈·주석 키)를 확인했다 | `inspect_dist.py` | research.md §실물 |
| 3 | **서버를 실제로 띄웠다.** 데몬과 같은 방식으로 띄워 준비·정상 종료 줄 원문, `help` 출력, 저장 뒤 생긴 파일을 기록했다. 다른 OS 빌드밖에 못 돌려도 돌린다 | `probe_server.py` | research.md §실물 |
| 4 | **명령은 `help`와 한 줄씩 맞다.** 서버가 모르는 명령이 없고, 아는 명령이 빠지지 않았다. 모든 명령에 `risk`·`description`이 있다 | `probe_server.py --send help`, `check.py` conformance, `trial.py` help | [manifest.md](references/manifest.md) |
| 5 | **콘솔 입력이 먹는다.** 파이프 stdin으로 보낸 명령에 답하는지 봤다. 아니면 `[launch] console = "pty"`로 다시 쟀다 | `probe_server.py` ("답 없음") | research.md §실물 |
| 6 | **실행 전제가 포터블이다.** 배포물 밖의 런타임을 찾았고, 고정 판을 받아 서버 폴더에 두며, 체크섬(없으면 서명 확인한 파일의 고정 SHA-256)으로 검증한다 | 직접 확인 | [lifecycle.md](references/lifecycle.md) §공통 |
| 7 | **설치는 이 OS 것만 푼다.** 여러 OS 폴더가 든 배포물에서 OS 폴더 이름으로 고른다 | `inspect_dist.py` OS 폴더, `trial.py` install | lifecycle.md §설치 |
| 8 | **설정 파일은 원래 형식 그대로 왕복한다.** 읽은 인코딩·줄바꿈·문법으로 다시 쓰고, 못 읽으면 쓰지 않는다. 공식 파일의 원래 바이트로 테스트했다 | `inspect_dist.py` 인코딩, `check.py` conformance(왕복), `trial.py` settings-write | lifecycle.md §설정 |
| 9 | **지우는 것은 계산된 대상만.** 초기화가 회전 백업까지 포함한 서버 생성 파일만 지우고, 서버 폴더가 아니면 거절한다 | 모듈 테스트, `trial.py` reset | lifecycle.md §파괴 작업 |
| 10 | **격리 실행에서 동작한다.** 새 모듈은 "확인 안 됨"으로 AppContainer 안에서 돈다. msiexec·서비스·`tempfile.mkdtemp` 폴더를 쓰지 않고, PC 실측을 격리 실행으로 했다 | `trial.py` (Windows) | [testing.md](references/testing.md) |
| 11 | **검사를 돌렸다.** 함수별 테스트(먼저 쓰고 실패 확인), `module validate`, 적합성 검사, `module audit` | `check.py` | testing.md |
| 12 | **위험 패턴 표를 한 줄씩 지웠다.** "해당 없음"도 이유를 적는다 | 직접 확인 | [lessons.md](references/lessons.md) |

Windows에서만 드러나는 위험은 Linux에서 보이지 않는다: 런타임 누락(XNA류), 파이프 입력 무시, 격리 권한, 레거시 인코딩(cp1252),
Windows 폴더 구조. 이 다섯은 Windows 실측 전까지 "확인 못 함"으로 보고한다.

## 작업 순서

도구는 Python 3.11 이상 표준 라이브러리만 쓴다. 명령은 모듈 저장소 루트에서 실행한다.

1. **기준 문서를 읽는다.** 계약의 유일한 기준은 saba-chan 저장소 `docs/architecture/02-modules.md`다. 이 저장소의 `docs/module-spec.md`(키·함수 표)와
   `docs/module-development-guide.md`(만드는 순서, 공용 도우미 표)는 요약이다. 다르면 02 문서와 데몬 코드(`src/supervisor/module_loader.rs`)를 믿는다.
   스키마는 `schema/module.schema.json`(= `saba-core module schema`). 저장소 `AGENTS.md`의 역할·검증 규칙도 따른다.
2. **뼈대를 만든다** (새 모듈만).
   ```
   python maker/new_module.py <name> --game-name "<게임 이름>"
   ```
   `__example/` 틀이 `<name>/`으로 복사되고, `[module]` 이름·설명·별명, 10개 언어 파일, 조사 표 `RESEARCH.md`, `pytest.ini` 등록이 채워진다.
   실행 파일 이름 같은 나머지 자리표시(`MyGameServer`)는 조사로 채운다.
3. **조사한다** → [references/research.md](references/research.md). 기억이 아니라 웹 검색으로 공식 출처를 찾고, 결과를 `RESEARCH.md` 표에 출처와 함께 적는다.
4. **실물을 본다.** 서버 배포물을 받아(공식 주소) 풀지 않고 점검한다.
   ```
   python maker/inspect_dist.py <zip | tar | 풀어 둔 폴더> --out inspect.md
   ```
   OS 폴더, 같은 이름의 실행 파일, 동봉 설정 파일의 인코딩·줄바꿈·기본값·주석 처리된 예시 키가 나온다. 결과를 조사 표에 "실측"으로 옮긴다.
5. **서버를 띄워 본다.** 실행 전제 런타임은 고정 판을 받아 둔 경로로 준다(PC에 설치된 것을 쓰지 않는다).
   ```
   python maker/probe_server.py --cwd <서버 폴더> --ready "<준비 줄 정규식>" --send help --send <확인할 명령>... \
       --stop <정지 명령> --out probe -- <실행 파일> <인자...>
   ```
   준비 줄, 명령마다의 응답, 정지 뒤 줄과 종료 코드, 생기거나 바뀐 파일이 원문으로 남는다(`probe/probe-output.log`는 원래 바이트).
   "답 없음"이면 파이프 입력을 읽지 않는 서버일 수 있으니 `[launch] console = "pty"`를 검토한다. 약관(EULA)이 있으면 사용자 동의를 먼저 받는다.
6. **module.toml을 채운다** → [references/manifest.md](references/manifest.md). 선언으로 되는 것은 선언으로.
7. **lifecycle.py를 쓴다** → [references/lifecycle.md](references/lifecycle.md). 함수마다 테스트를 먼저 쓰고 실패를 확인한 뒤 구현한다([references/testing.md](references/testing.md)).
8. **관문을 검사한다.**
   ```
   python maker/check.py <name> --saba-core <saba-core 실행 파일>
   ```
   자리표시·조사 표 출처·10개 언어·테스트 등록·버전, 모듈 테스트, 적합성 키트, `module validate`, `module audit`를 돌려 표로 보고한다.
   실패나 건너뛴 관문(`--saba-core`를 주지 않으면 validate·audit)이 있으면 종료 코드 1이다. 번역하지 않고 en.json 그대로 둔 언어 파일도 실패다.
   "직접 확인" 목록은 에이전트가 스스로 확인하고 결과를 적는다.
9. **실측한다.** 진짜 데몬과 격리로 설치 → 설정 → 시작 → 명령 → 정지 → 백업 → 초기화를 밟는다.
   ```
   python maker/check.py <name> --saba-core <saba-core> --trial [--accept-agreement <id>]
   ```
   (`conformance/trial.py`를 부른다.) Windows에서 설치된 사바쨩의 `saba-core.exe`(옆에 `saba-runner.exe`)를 주면 실제 AppContainer 격리로 돈다.
   설치된 사바쨩의 데이터·포트는 건드리지 않는다. `trial-report/trial-report.md`를 PR 본문에 붙인다.
10. **마무리** → testing.md §마무리. 보고에 조사 표, 옮기지 못한 것과 이유, 확인 못 한 것, 실행한 검사와 결과를 적는다.
    커밋·PR은 사용자가 요청했을 때만. 버전은 올리지 않는다.

## 도구 요약

| 도구 | 하는 일 |
|---|---|
| `maker/new_module.py` | `__example` 틀로 새 모듈 뼈대 + `RESEARCH.md` + 10개 언어 + `pytest.ini` 등록 |
| `maker/inspect_dist.py` | 배포물 항목 전체, OS 폴더, 같은 이름 실행 파일, 설정 파일 원래 바이트(인코딩·줄바꿈·키) |
| `maker/probe_server.py` | 서버를 파이프로 띄워 준비·명령 응답·정지·생긴 파일을 원문으로 기록 |
| `maker/check.py` | PR 전 관문 중 기계로 잴 수 있는 것 전부 + 직접 확인 목록, `--trial`이면 실측까지 |
| `conformance/trial.py` | 진짜 데몬·러너·격리로 사용자 흐름 전체를 실측 (`conformance/README.md`) |

## 조사 원칙 (모든 단계에 적용)

1. **추측 금지.** 모든 명령·설정 키·기본값·포트·파일 경로·종료 문구는 출처(공식 문서 URL, 서버가 실제로 만든 파일, 실측 로그) 하나 이상에 묶는다. 출처를 찾지 못한 항목은 비워 두고 "확인 못 함"으로 보고한다.
2. **문서보다 실물.** 배포물 목록, 동봉 기본 파일의 원래 바이트, `help` 출력, 콘솔 문구 원문이 문서·위키·커뮤니티 자료보다 우선한다. 위키는 옛 버전 기준일 수 있다.
3. **기억이 아니라 공식 자료.** 게임 정보는 기억으로 쓰지 않는다. 매번 웹 검색으로 공식 출처를 찾아 읽고 URL을 조사 표에 남긴다. 공식 출처가 없을 때만 커뮤니티 자료를 쓰고, 그때는 실측으로 한 번 더 확인한다.
4. **공식 기능이 먼저.** 게임이 명령·API·실행 인자·기본 파일·백업 절차를 공식으로 제공하면 그것을 쓴다. kill·sleep·파일 짐작으로 대신하지 않는다.
5. **전부 지원.** 게임 서버가 가진 명령과 사용자가 조정할 수 있는 설정은 빠짐없이 옮긴다. 옮기지 못한 것은 목록과 이유를 보고하고, 데몬에 범용 지점을 먼저 만든다.
6. **다른 모듈과 같은 방식으로.** 같은 일(정지, 초기화, 백업, 접속자, 진단, 기본값)은 다른 모듈이 어떻게 하는지 먼저 열어 보고 맞춘다([lessons.md](references/lessons.md) §모듈 간 불일치).

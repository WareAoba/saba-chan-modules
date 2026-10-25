# 모듈 메이커

사바쨩 게임 서버 모듈을 만드는 에이전트용 스킬과 도구 모음이다. Claude, Codex, GPT 등 어느 에이전트든 같은 절차로 모듈을 만들고
고치고 검사하게 한다. 사람이 직접 써도 된다.

- [SKILL.md](SKILL.md): 절차 본문 (설계 원칙, PR 전 관문, 작업 순서). [Agent Skills](https://agentskills.io) 형식이다.
- [references/](references/): 단계별 자세한 규칙 (조사, module.toml, lifecycle.py, 테스트·실측, 지난 사고)
- 도구 (Python 3.11 이상, 표준 라이브러리만):

| 도구 | 하는 일 |
|---|---|
| `new_module.py <name> --game-name "<이름>"` | `__example` 틀로 새 모듈 뼈대, 조사 표 `RESEARCH.md`, 10개 언어, `pytest.ini` 등록 |
| `inspect_dist.py <zip·tar·폴더>` | 배포물 항목 전체, OS 폴더, 같은 이름 실행 파일, 설정 파일 원래 바이트(인코딩·줄바꿈·키) |
| `probe_server.py --cwd <폴더> ... -- <실행 파일>` | 서버를 파이프로 띄워 준비 줄, 명령 응답, 정지 뒤 줄, 생긴 파일을 원문으로 기록 |
| `check.py <name> [--saba-core ...] [--trial]` | PR 전 관문 중 기계로 잴 수 있는 것 전부와 직접 확인 목록, `--trial`이면 `conformance/trial.py` 실측까지 |

## 에이전트에 연결하기

이 저장소에서 일하는 에이전트는 따로 설치할 것이 없다. `AGENTS.md`가 모듈 작업에 이 스킬을 쓰라고 가리키고,
Claude Code는 `.claude/skills/saba-module-maker/`로도 찾는다.

다른 곳에서도 스킬로 부르려면 이 폴더를 에이전트의 스킬 폴더에 연결한다. 복사하지 않고 연결하면 저장소를 받을 때마다 같이 갱신된다.
도구는 모듈 저장소 안에서 실행하므로 저장소는 받아 둔다.

```powershell
# Windows (관리자 권한 없이 되는 디렉터리 연결)
cmd /c mklink /J "%USERPROFILE%\.claude\skills\saba-module-maker" "C:\Git\saba-chan-modules\maker"
cmd /c mklink /J "%USERPROFILE%\.codex\skills\saba-module-maker" "C:\Git\saba-chan-modules\maker"
```

```bash
# Linux·macOS
ln -s "$PWD/maker" ~/.claude/skills/saba-module-maker
ln -s "$PWD/maker" ~/.codex/skills/saba-module-maker
```

스킬 폴더를 읽지 않는 에이전트에는 프롬프트로 `maker/SKILL.md`를 먼저 읽고 따르라고 한다.

## 테스트

```bash
pytest maker
```

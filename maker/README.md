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

## 받기

메이커의 버전은 `.claude-plugin/plugin.json` 하나에 있다. 발행할 때 한 번 올리고 `maker-v<버전>` 태그를 단다.
Claude Code 플러그인 마켓(`.claude-plugin/marketplace.json`)은 아직 열지 않았다. 열 때는 저장소 루트에 마켓 파일만 더하면 이 폴더가 그대로 플러그인이 된다.
이 메이커가 맞는 데몬 계약은 `SKILL.md` 머리의 `metadata`(saba-core 최소 버전, 모듈 스키마 버전)다.

| 에이전트 | 설치 | 업데이트 |
|---|---|---|
| Claude Code | `git clone https://github.com/WareAoba/saba-chan-modules` 뒤 그 폴더에서 Claude Code를 연다. `.claude/skills/saba-module-maker/`로 스킬을 찾는다 | `git pull` |
| Codex | 기본 스킬 `$skill-installer`에 "WareAoba/saba-chan-modules 저장소의 maker 경로 설치"라고 부탁 (`--repo WareAoba/saba-chan-modules --path maker`) | 같은 방법으로 다시 설치 |
| 그 밖의 에이전트 | 이 저장소를 받으면 `AGENTS.md`가 `maker/SKILL.md`를 가리킨다. 스킬 폴더를 읽지 않으면 "`maker/SKILL.md`를 읽고 따르라"고 부탁 | `git pull` |

어떤 길로 받든 도구는 받아 둔 모듈 저장소 안에서 실행한다(`SKILL.md` 작업 순서 0). 이 저장소에서 일하는 Claude Code는
`.claude/skills/saba-module-maker/`로도 이 스킬을 찾는다.

사바쨩이 설치된 PC면 `saba-chan-cli module maker`가 설치 명령, 첫 부탁 예시, 이 PC의 경로를 보여 준다.
## 테스트

```bash
pytest maker
```

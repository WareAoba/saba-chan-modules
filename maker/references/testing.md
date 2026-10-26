# 테스트와 실측 (실행한 것만 통과로 적는다)

1. **함수마다 테스트를 먼저 쓴다**(`<game>/test_lifecycle.py`, pytest). 실패하는 것을 확인한 뒤 구현한다. FUNCTIONS의 모든 함수와 모든 `module` 명령에 정상·실패 경우를 하나 이상. 실제 서버 없이 config를 넣고 답과 파일 변화를 본다. lifecycle.md의 설정 왕복, 기존 파일 보존, 백업 경로, 파괴 범위 테스트를 포함한다. 구현을 그대로 베낀 테스트, 커버리지만 늘리는 테스트는 쓰지 않는다.
2. **관문 검사**: `python maker/check.py <game> --saba-core <saba-core>`. 아래를 한 번에 돌린다.
   - `pytest <game>` (모듈 테스트)
   - `pytest conformance --module <game>` (적합성 키트, `conformance/README.md`). 저장소 밖 모듈이면 `--shared-libs <saba-chan>/modules`
   - `saba-core module validate <game>` (데몬과 같은 파서)
   - `saba-core module audit <game>` (찾은 능력이 `[permissions]`와 맞는지)
   - 정적 관문: 틀 자리표시, 조사 표 출처 URL, 10개 언어와 ko 번역, 테스트 등록, 버전을 올리지 않았는지, 틀 예시가 아닌 하이라이트 규칙, download 설치의 `get_installed_version`
3. **손 확인**: 조사 표의 명령·설정이 모두 module.toml에 있는지, 명령 이름이 공식 목록과 같은지, 모든 명령에 `risk`·`description`이 있는지, 설정 `type`이 6가지 안인지, lessons.md 표를 하나씩 지웠는지. `check.py`의 "직접 확인" 목록을 채운다.
4. **제작 환경 실측**(PR 전 필수): 실물 확인(research.md)을 끝낸 상태여야 한다. 그 위에서 진짜 데몬으로 사용자 흐름을 밟는다.
   ```
   python maker/check.py <game> --saba-core <saba-core> --trial [--accept-agreement <id>] [--extensions <saba-chan-extensions>]
   ```
   `conformance/trial.py`가 임시 데이터 폴더와 빈 포트로 데몬을 하나 더 띄워 설치 → 설정 → 시작 → 준비 → help 대조 → 명령 → 정지 → 백업 → 초기화를 잰다. `saba-runner`가 `saba-core` 옆에 있어야 한다. 약관은 사용자가 동의한 것만 `--accept-agreement`로 준다. 단위 테스트만 통과한 상태를 "실측"이라 쓰지 않는다.
5. `[module.dependencies] saba-core`가 아직 발행되지 않은 버전이면 정식 설치는 그 모듈을 거절한다(버전 검사). 그때는 PC 실측을 그 버전 발행 뒤로 미루고 보고에 적는다. 모듈 쪽에서 의존 버전을 낮추거나 데몬 버전 표기를 바꿔 우회하지 않는다.
6. 새 모듈은 공식 카탈로그에 오르기 전까지 "확인 안 됨"으로 격리(AppContainer)되어 돈다. 허용된 폴더만 쓸 수 있고 파일 권한 변경(WRITE_DAC)이 없다. 단위 테스트는 이 차이를 보지 못하므로 PC 실측은 격리 실행으로 한다: Windows에서 설치된 사바쨩의 `saba-core.exe`로 `trial.py`를 돌린다. 격리 때문에 실패하면 모듈에서 우회하지 말고 데몬·공용 도우미 문제로 보고한다(테라리아 실측: 공용 `atomic_write`가 기존 파일을 바꾸지 못함).
7. **마지막에 PC 실측**: Windows PC의 정규 설치 앱(정규 경로)에서 실제 서버로 설치 → 시작 → 명령 전부(특히 추방·밴은 실제 대상에게) → 설정 변경 후 재시작해 서버가 그 값을 쓰는지 → 백업·복원 → 초기화 → 정지 후 세이브 확인. 시험용 폴더·별도 설치를 만들지 않는다. 클라우드 검사만으로 끝났다고 쓰지 않는다.

## 마무리

- 데몬 계약이 모자라면(새 키, 새 타입, 백업 전·후 명령, 초기화 대상 선언 등) 모듈에서 우회하지 말고 데몬에 범용 지점을 먼저 만든다. 계약을 바꾸면 같은 커밋에서 saba-chan `docs/architecture/02-modules.md`와 이 저장소의 스키마·문서를 고친다.
- 보고에는 조사 표(출처 포함), 옮기지 못한 명령·설정과 이유, 확인 못 한 항목, 다른 모듈과 방식이 다른 곳, 실행한 검사와 결과(`check.py` 표, `trial-report.md`)를 적는다. 조사 표의 근거는 "문서"와 "실측"을 구별한다. 실측 근거가 없는 콘솔 문구·명령은 넣지 않는다.
- 커밋·PR은 사용자가 요청했을 때만. 버전은 올리지 않는다(발행 때 한 번). 동결 중이면 PR만 열고 병합은 발행 담당에게 맡긴다.
- 저장소 `AGENTS.md`가 정한 교차 검증(역할과 협업 절)을 받고, 확인된 문제만 고친다.

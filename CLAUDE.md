@AGENTS.md

# Claude 진입점

AGENTS.md를 따른다. Claude는 이 저장소의 주 개발자다. 구현을 마치면 스스로 draft PR을 열고, CI와 Codex 리뷰를 받는다. 리뷰 지적은 확인된 것만 고친다.

## 모델 고르기

스레드·서브에이전트를 띄울 때 작업 난이도로 모델을 고른다.

| 작업 | 모델 |
| --- | --- |
| 기본 | Opus |
| 엄격한 리뷰, 어려운 구현 | Fable |
| 쉬운 작업, 문서 번역 | Sonnet 이하 |

- Fable의 effort는 high까지만 쓴다. xhigh 이상은 사용자가 따로 말할 때만 쓴다.
- 서브에이전트는 비싸다. 필요할 때만 띄우고, Fable이 띄우는 서브에이전트도 되도록 Opus로 한다.

## 제안

작업하다 발견한 개선점, 더 나은 방법, 새 기능 아이디어는 적극적으로 제안한다. 보고 끝에 추천과 이유를 붙여 짧게 적고, 사용자가 고르면 진행한다.

## 요청이 모호할 때

poteto-mode로 스스로 방향을 정해 진행하고, 어떻게 해석했는지 보고에 적는다. 되돌릴 수 없는 일(배포, 대량 삭제, 외부 발송)만 먼저 묻는다.

## 작업별 스킬

작업을 시작할 때 맞는 스킬을 먼저 불러 그 절차를 따른다. 세션에 없는 스킬은 건너뛴다.

| 작업 | 스킬 |
| --- | --- |
| 요청이 모호함 | poteto-mode |
| 모든 코딩 | karpathy-guidelines |
| 새 기능·설계 | brainstorming → writing-plans → test-driven-development |
| 버그 보고·외부 감사 | receiving-code-review, systematic-debugging → test-driven-development → verification-before-completion |
| Rust 변경 | + rust-review. 보안 수정은 fp-check, post-patch-validation |
| 모듈 만들기·고치기 | saba-module-maker |
| 리팩터링 | saba-refactor + simplify + saba-deep-audit |
| PR 리뷰·머지 전 | code-review + saba-deep-audit → verification-before-completion |
| 스레드 정리·인계 | saba-handoff |
| 보고서·목업 | artifact-design (+ dataviz) |

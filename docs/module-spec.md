# 모듈 계약 안내

모듈 계약(`module.toml` 키, 진입 파일 함수와 응답 모양)의 기준은 saba-chan 저장소의
[02-modules.md](https://github.com/WareAoba/saba-chan/blob/main/docs/architecture/02-modules.md)다. 이 저장소에는 사본을 두지 않는다.

- 키와 값 규칙은 데몬 파서에서 뽑은 스키마 `schema/module.schema.json`(= `saba-core module schema`)이다.
  손으로 고치지 말고 `saba-core module schema > schema/module.schema.json`으로 다시 만든다.
- 데몬과 같은 코드로 검사한다: `saba-core module validate <모듈 폴더>`(실패하면 종료 코드 1), `saba-core module audit <모듈 폴더>`.
- 이 저장소의 적합성 키트([conformance/](../conformance/README.md))는 데몬보다 조금 더 요구한다. 예: 명령마다 `risk`와 `description`,
  10개 언어 파일의 같은 키.
- 만드는 절차와 PR 전 관문은 [maker/SKILL.md](../maker/SKILL.md), 위험도 기준은 [maker/references/manifest.md](../maker/references/manifest.md).

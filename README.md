# saba-chan-modules

[사바쨩](https://github.com/WareAoba/saba-chan)의 게임 서버 모듈 모음입니다. 각 모듈은 `<game>/module.toml` + `<game>/lifecycle.py` (+ `locales/`)로 이루어진 게임 서버 구동 "설명서"입니다.

모듈 스키마와 데몬 호출 프로토콜은 코드 기준 문서 [saba-chan docs/architecture/02-modules.md](https://github.com/WareAoba/saba-chan/blob/main/docs/architecture/02-modules.md)만 기준으로 삼습니다. 이 저장소의 [모듈 계약](docs/module-spec.md)과 [모듈 만들기 안내](docs/module-development-guide.md)도 같은 계약을 따릅니다. 새 모듈은 `__example/`에서 시작하고, `pytest conformance`로 적합성을 검사하세요.

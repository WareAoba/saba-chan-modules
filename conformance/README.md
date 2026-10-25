# 모듈 적합성 키트 (M10)

모듈이 데몬과의 계약(`docs/module-spec.md`)을 지키는지 검사한다. 이 저장소의 CI가 PR마다 돌린다.

```bash
pip install pytest jsonschema
# 이 저장소의 모든 모듈 (saba-chan 저장소를 옆에 받아 두었을 때)
pytest conformance
# 다른 곳에서 만든 모듈 하나
pytest conformance --module path/to/mygame --shared-libs path/to/saba-chan/modules
```

`--shared-libs`(또는 `SABA_SHARED_LIBS`)는 `i18n.py`, `daemon_rcon.py`가 있는 saba-chan 저장소의 `modules/` 폴더다. 데몬 실행 파일도 같은 규칙으로 검사할 수 있다: `saba-core module validate <모듈 폴더>`.

| 검사 | 실패 뜻 |
|---|---|
| `test_manifest_matches_schema` | `module.toml`이 스키마 v1(`schema/module.schema.json`)과 다르다 |
| `test_folder_name_is_module_name` | 폴더 이름과 `[module] name`이 다르다 |
| `test_entry_file_exists` | `[module] entry` 파일이 없다 |
| `test_commands_declare_risk_and_description` | `[[commands.fields]]`의 명령에 `risk`나 `description`이 없다 (risk가 없으면 데몬은 조용히 caution으로 본다) |
| `test_protocols_follow_the_priority` | `[protocols] supported`에 `stdin`·`rest`가 있는데 `rcon`도 선언했거나(다른 경로가 있으면 RCON은 쓰지 않는다), `rcon`만 선언하고 `rcon_port`가 없다 |
| `test_functions_table` | `FUNCTIONS`에 `get_launch_command`가 없거나, 데몬이 부르는 이름으로 만든 함수를 표에 안 넣었거나, 훅(`[backup]`·`[commands]` `hook = true`)이나 `[[queries]]`·`[status] players`가 가리키는 함수가 표에 없다 |
| `test_unknown_function_is_answered` | 모르는 함수에 `{"success": false, "message": "Unknown function: X"}`로 답하지 않는다 |
| `test_safe_call_keeps_the_contract` | 빈 작업 폴더에서 `get_launch_command` `validate` `status` `read_properties` `diagnose_log` `get_installed_version`을 불렀을 때 30초 안에 종료 코드 0으로 끝나지 않거나, stdout 마지막 줄이 JSON 객체가 아니거나, `success`가 bool이 아니거나, 실패 이유가 없거나, `PROGRESS:` 줄이 JSON이 아니다 |
| `test_functions_call_lists_the_table` | 예약 함수 `functions`가 `{"success": true, "functions": [...]}`로 `FUNCTIONS` 표의 이름을 그대로 돌려주지 않는다 (`run_lifecycle`을 쓰면 공용 파일이 답한다) |
| `test_settings_round_trip` | 새 작업 폴더에서 `configure`가 실패하거나, 설정 기본값을 바꿔(boolean 뒤집기, number ±1, select 다른 값) `configure`로 쓴 뒤 `import_settings`로 읽은 값이 다르거나(예: 숫자·불리언을 문자열로 돌려줌) 바꾼 설정을 돌려주지 않는다. 게임 파일에 두지 않는 설정(`group = "saba-chan"`)은 넣지도 비교하지도 않고, 익스텐션이 필요하면 건너뛴다 |
| `test_plain_stop_never_kills_without_saving` | 콘솔이 없는(데몬이 닫힌) 서버에 `force: false` 정지를 했을 때, 끝냈다면서 프로세스가 살아 있거나, `graceful_stop_unavailable`라면서 프로세스를 끝냈거나 `success: false`·`message`가 없다 |
| `test_translation_keys` | 코드가 쓰는 번역 키가 `locales/en.json`에 없거나, 다른 언어 파일에 en에 없는 키가 있다. 번역 안 된 키는 경고만 한다 |
| `test_code_uses_only_declared_permissions` | 코드가 쓰는 능력(네트워크, 프로세스 실행, 네이티브·동적 코드 등)을 `[permissions]`에 선언하지 않았다 (`saba_audit.py`가 있는 saba-chan일 때) |

키트는 익스텐션을 넣지 않고 데몬 주소를 닫힌 포트로 준다. `HOME`·`USERPROFILE`도 임시 폴더로 바꿔, 사용자 폴더를 기본 위치로 쓰는 게임도 임시 폴더에만 쓰게 한다. 익스텐션이나 데몬이 없어도 모듈은 죽지 않고 JSON으로 답해야 한다(`error_code: "extension_required"`, `missing_extensions[]`).

## 실측 모드 (`trial.py`)

위 검사는 빈 작업 폴더에서 함수를 한 번씩 부른다. 실제 배포판, 실제 콘솔, 실제 격리에서만 드러나는 문제(설치 파일 이름, 의사 콘솔, 격리 안의 권한 거부, msiexec 1601 등)는 `trial.py`로 잰다. PR을 올리기 전에 돌리고 `trial-report.md`를 PR 본문에 붙인다.

```bash
python conformance/trial.py path/to/mygame --saba-core path/to/saba-core \
    [--extensions path/to/saba-chan-extensions] [--accept-agreement eula] [--out trial-report]
```

- 진짜 데몬(`saba-core`)을 임시 데이터 폴더로 하나 더 띄우고(설치된 사바쨩과 데이터·포트를 나누지 않는다) 데몬 API만 부른다. `saba-runner`가 `saba-core` 옆에 있어야 한다. 끝나면 임시 폴더를 지운다(`--keep`이면 남긴다).
- 모듈은 임시 모듈 폴더에 복사되어 확인되지 않은 모듈이 된다. Windows에서는 모든 lifecycle 호출이 데몬의 실제 AppContainer 격리 안에서 돈다. 격리가 없는 OS에서는 그 복사본에만 비격리 동의를 하고 돌리며 보고서에 "격리 없음"으로 적는다.
- 흐름: 데몬 기동과 모듈 로드 → 버전 목록 → 인스턴스 생성과 실제 배포판 설치 → 설치 버전 → 게임 파일 설정 읽기, 숫자 설정 하나 쓰고 다시 읽기, 속성 읽기 → 시작 전 점검과 시작(러너, `[launch] console`) → `ready_patterns` 준비 알림 → `--help-command`(기본 `help`) 출력과 선언한 콘솔 명령 대조 → 명령 목록, 접속자, 로그 진단 → 저장 후 정지(종료 판정 `user_stop`, `graceful_exit_patterns`, 정지 중 저장된 파일) → 백업(서버가 쓴 플레이 데이터가 백업 대상에 다 들었는지, 서버 실행 파일이 들었는지) → 초기화(지운 파일, 서버 실행 파일을 지웠는지).
- 약관(`[[agreements]]`)은 `--accept-agreement`로 준 것만 동의한다. 동의하지 않은 약관이 있으면 시작 단계부터 건너뛰고 초기화만 잰다.
- 실패한 응답이 격리 문제로 보이면(`sandbox_unavailable`, 권한 거부, 1601 등) 그 단계에 "격리 오류 의심"을 적는다.
- 결과는 `<out>/trial-report.md`, `trial-report.json`, `daemon.log`다. 실패가 하나라도 있으면 종료 코드 1이다. 경고는 사람이 판단할 것(help 문구에 없는 명령, 백업 대상 밖의 데이터 등)이다.
- `pytest conformance --saba-core <saba-core>`(또는 `SABA_CORE`)를 주면 `test_trial.py`가 픽스처 모듈(`fixtures/trialgame`)로 이 흐름 전체를 돌린다. 주지 않으면 판정 규칙만 검사한다.

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

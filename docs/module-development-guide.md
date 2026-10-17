# saba-chan 모듈 개발 가이드

> **saba-chan**은 게임 서버의 생명주기를 관리하는 데몬입니다.  
> 모듈은 특정 게임 서버를 saba-chan으로 제어할 수 있도록 하는 플러그인입니다.  
> 이 문서는 새로운 모듈을 작성하기 위한 완전한 가이드입니다.

---

## 목차

1. [개요](#1-개요)
2. [빠른 시작 — 5분 안에 첫 모듈 만들기](#빠른-시작--5분-안에-첫-모듈-만들기)
3. [모듈 구조](#2-모듈-구조)
4. [module.toml 완전 레퍼런스](#3-moduletoml-완전-레퍼런스)
   - 3.1 [module — 메타데이터](#31-module--메타데이터)
   - 3.2 [update — 자동 업데이트](#32-update--자동-업데이트)
   - 3.3 [protocols — 통신 프로토콜](#33-protocols--통신-프로토콜)
   - 3.4 [credential_map — 자격증명 동기화](#34-credential_map--자격증명-동기화)
   - 3.5 [config — 기본 실행 설정](#35-config--기본-실행-설정)
   - 3.6 [install — 서버 설치 방법](#36-install--서버-설치-방법)
   - 3.8 [detection — 자동 감지](#38-detection--자동-감지)
   - 3.9 [settings — GUI 설정 필드](#39-settings--gui-설정-필드)
   - 3.10 [commands — 서버 명령어](#310-commands--서버-명령어)
   - 3.11 [errors — 에러 메시지](#311-errors--에러-메시지)
   - 3.12 [aliases — 별칭](#312-aliases--별칭)
   - 3.13 [syntax_highlight — 콘솔 구문 강조](#313-syntax_highlight--콘솔-구문-강조)
4. [lifecycle.py 완전 레퍼런스](#4-lifecyclepy-완전-레퍼런스)
   - 4.1 [기본 구조](#41-기본-구조)
   - 4.2 [config 객체](#42-config-객체)
   - 4.3 [필수 함수](#43-필수-함수)
   - 4.4 [선택 함수](#44-선택-함수)
   - 4.5 [반환값 규칙](#45-반환값-규칙)
5. [i18n (다국어 지원)](#5-i18n-다국어-지원)
6. [데몬 API 연동](#6-데몬-api-연동)
   - 6.1 [daemon_rcon — RCON 브릿지](#61-daemon_rcon--rcon-브릿지)
   - 6.2 [환경 변수](#62-환경-변수)
7. [익스텐션 활용](#7-익스텐션-활용)
8. [설정 키 매핑 패턴](#8-설정-키-매핑-패턴)
9. [프로토콜별 구현 가이드](#9-프로토콜별-구현-가이드)
   - 9.1 [RCON 모듈 (Minecraft, Zomboid)](#91-rcon-모듈-minecraft-zomboid)
   - 9.2 [REST API 모듈 (Palworld)](#92-rest-api-모듈-palworld)
   - 9.3 [stdin 모듈 (콘솔 모드)](#93-stdin-모듈-콘솔-모드)
10. [설치 방식별 구현](#10-설치-방식별-구현)
11. [에러 진단 시스템](#11-에러-진단-시스템)
12. [테스트 작성](#12-테스트-작성)
13. [체크리스트](#13-체크리스트)
14. [실전 예제: 최소 모듈](#14-실전-예제-최소-모듈)
15. [실전 예제: 전체 기능 모듈](#15-실전-예제-전체-기능-모듈)

---

## 1. 개요

### 모듈이란?

saba-chan 모듈은 특정 게임 서버의 생명주기를 관리하는 Python 스크립트와 TOML 설정 파일의 묶음입니다. 데몬(Rust)이 모듈을 로드하고, 서버 시작/종료/상태 조회/설정 적용 등의 작업을 모듈에 위임합니다.

### 아키텍처

```
┌─────────────────────────────────────────────────┐
│  saba-chan 데몬 (Rust)                            │
│                                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │ Instance │  │ Instance │  │   ManagedProcess  │ │
│  │ Manager  │──│  Config  │──│  (프로세스 관리)   │ │
│  └──────────┘  └──────────┘  └──────────────────┘ │
│       │                            │               │
│       ▼                            ▼               │
│  ┌──────────────────────────────────────┐          │
│  │  Python Plugin Runner                │          │
│  │  lifecycle.py <function> < config    │          │
│  └──────────────────────────────────────┘          │
└─────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│  모듈 (Python)            │
│  ├─ module.toml          │  ← 메타데이터, 설정 스키마
│  ├─ lifecycle.py         │  ← 생명주기 로직
│  ├─ locales/             │  ← 다국어 번역
│  └─ icon.png             │  ← 아이콘
└─────────────────────────┘
```

### 통신 흐름

1. 데몬이 `lifecycle.py <function_name>` 을 자식 프로세스로 실행
2. config JSON을 **stdin**으로 전달
3. 모듈은 결과를 **stdout**에 JSON으로 출력
4. 로그는 **stderr**로 출력 (`print(..., file=sys.stderr)`)

> **중요**: stdout에는 반드시 유효한 JSON만 출력해야 합니다. `print()` 디버그 메시지는 반드시 `file=sys.stderr`를 사용하세요.

### 데몬이 함수를 호출하는 순서

각 상황에서 데몬이 어떤 함수를 어떤 순서로 호출하는지 알아야 모듈을 올바르게 구현할 수 있습니다.

```
■ 서버 시작
  ┌─ validate(config)               검증 실패 시 여기서 중단
  │    ↓ success=True
  ├─ configure(config)              설정 파일에 값 적용
  │    ↓
  └─ get_launch_command(config)     program, args 등 반환 → 데몬이 spawn

■ 서버 종료
  └─ stop(config)                   PID, force 플래그 전달

■ 서버 상태 조회
  └─ status(config)                 PID 전달, running/stopped 반환

■ 명령어 실행 (예: say, kick)
  ├─ [rcon_template 있으면]          데몬이 직접 RCON 전송 (lifecycle.py 안 거침)
  └─ [rcon_template 없으면]          command(config) 호출

■ 인스턴스 최초 등록 / 기존 서버 연결
  └─ import_settings(config)        게임 설정 파일 → saba-chan 키로 가져오기

■ 사용자가 GUI에서 설정 변경
  └─ configure(config)              변경된 settings 적용

■ 서버 설치 (install 지원 모듈)
  └─ install_server(config)         install_dir, version 전달
```

> **핵심 포인트**: `get_launch_command()`는 서버를 직접 실행하지 않습니다. 실행 파일 경로와 인자만 반환하면 데몬이 알아서 프로세스를 spawn하고 관리합니다.

### config 객체 실제 예시

데몬이 `get_launch_command` 호출 시 stdin으로 전달하는 JSON의 실제 모습:

```json
{
  "instance_id": "a1b2c3d4-5678-90ef-ghij-klmnopqrstuv",
  "server_executable": "C:\\GameServers\\MyGame\\GameServer.exe",
  "working_dir": "C:\\GameServers\\MyGame",
  "port": 7777,
  "rcon_port": 25575,
  "rcon_password": "auto_generated_pw_abc123",
  "settings": {
    "server_name": "My Server",
    "max_players": 16,
    "pvp": true,
    "difficulty": "normal",
    "managed_start": true,
    "graceful_stop": true
  }
}
```

`stop` 호출 시:

```json
{
  "instance_id": "a1b2c3d4-5678-90ef-ghij-klmnopqrstuv",
  "server_executable": "C:\\GameServers\\MyGame\\GameServer.exe",
  "working_dir": "C:\\GameServers\\MyGame",
  "pid": 12345,
  "force": false,
  "graceful_stop": true,
  "rcon_port": 25575,
  "rcon_password": "auto_generated_pw_abc123",
  "port": 7777,
  "settings": { "...생략..." }
}
```

`command` 호출 시 (`rcon_template` 없는 커스텀 명령어):

```json
{
  "instance_id": "a1b2c3d4-...",
  "command": "backup",
  "args": { "slot": "1" },
  "rcon_port": 25575,
  "rcon_password": "auto_generated_pw_abc123",
  "working_dir": "C:\\GameServers\\MyGame",
  "settings": { "...생략..." }
}
```

---

## 빠른 시작 — 5분 안에 첫 모듈 만들기

> 전체 레퍼런스를 읽기 전에, 먼저 최소한의 모듈을 만들어서 동작을 확인하세요.  
> 아래 4단계만 따라하면 데몬에 모듈이 로드됩니다.

### Step 1: 폴더 만들기

`saba-chan-modules/` 디렉토리 안에 게임 이름으로 폴더를 만듭니다.

```
saba-chan-modules/
└── mygame/              ← 새로 만드는 폴더
    ├── module.toml      ← Step 2에서 작성
    └── lifecycle.py     ← Step 3에서 작성
```

### Step 2: module.toml — 최소 설정

**게임 정보를 모르겠으면 아래 4곳만 바꾸세요:**

```toml
[module]
name = "mygame"                         # ← 폴더명과 동일하게
display_name = "My Game"                 # ← GUI에 표시할 이름
entry = "lifecycle.py"
version = "0.1.0"

[protocols]
supported = ["rcon"]                     # ← 게임이 RCON 없으면 ["stdin"]으로
default = "rcon"
interaction_mode = "commands"            # ← stdin 쓰면 "console"로

[config]
server_executable = "GameServer.exe"     # ← 실제 실행 파일명
process_name = "GameServer"              # ← 작업관리자에 보이는 이름
default_port = 7777                      # ← 게임 기본 포트

[detection]
process_patterns = ["GameServer"]
dir_signatures = ["GameServer.exe"]

[settings]
[[settings.fields]]
name = "port"
type = "number"
label = "Game Port"
default = 7777
```

### Step 3: lifecycle.py — 최소 구현

**모든 함수가 dict를 반환하면 됩니다. 처음엔 get_launch_command만 제대로 구현하세요:**

```python
#!/usr/bin/env python3
import sys, json, os

def validate(config):
    exe = config.get("server_executable", "")
    if not exe or not os.path.exists(exe):
        return {"success": False, "issues": [{
            "code": "EXE_NOT_FOUND", "severity": "critical",
            "message": f"실행 파일을 찾을 수 없습니다: {exe}",
            "solution": "서버를 먼저 설치하거나 경로를 확인하세요."
        }]}
    return {"success": True, "issues": []}

def get_launch_command(config):
    exe = config.get("server_executable")
    port = config.get("port", 7777)
    wd = config.get("working_dir") or os.path.dirname(exe)
    return {
        "success": True,
        "program": os.path.abspath(exe),
        "args": [f"-port={port}"],           # ← 게임에 맞게 수정
        "working_dir": os.path.abspath(wd),
        "env_vars": {},
    }

def stop(config):
    pid = config.get("pid")
    if pid:
        os.system(f"taskkill /F /PID {pid}" if os.name == "nt" else f"kill -9 {pid}")
        return {"success": True, "message": "Server stopped"}
    return {"success": False, "message": "No PID"}

def status(config):
    pid = config.get("pid")
    if pid:
        try:
            os.kill(pid, 0)
            return {"success": True, "status": "running"}
        except OSError:
            pass
    return {"success": True, "status": "stopped"}

def configure(config):      return {"success": True}
def import_settings(config): return {"success": True, "settings": {}}
def command(config):         return {"success": True, "message": "OK"}

# ─── 아래는 수정하지 마세요 ───
FUNCTIONS = {
    "validate": validate, "get_launch_command": get_launch_command,
    "stop": stop, "status": status, "configure": configure,
    "import_settings": import_settings, "command": command,
}
if __name__ == "__main__":
    fn_name = sys.argv[1] if len(sys.argv) > 1 else ""
    cfg = json.loads(sys.stdin.read() or "{}")
    fn = FUNCTIONS.get(fn_name)
    print(json.dumps(fn(cfg) if fn else {"success": False, "message": f"Unknown: {fn_name}"}))
```

### Step 4: 동작 확인

데몬 없이도 터미널에서 직접 테스트할 수 있습니다:

```powershell
# Windows (PowerShell)
echo '{"server_executable": "C:\\GameServers\\MyGame\\GameServer.exe", "port": 7777}' | python lifecycle.py validate

# 예상 출력 (파일이 없으면):
# {"success": false, "issues": [{"code": "EXE_NOT_FOUND", ...}]}

echo '{"server_executable": "C:\\GameServers\\MyGame\\GameServer.exe", "port": 7777}' | python lifecycle.py get_launch_command

# 예상 출력:
# {"success": true, "program": "C:\\GameServers\\MyGame\\GameServer.exe", "args": ["-port=7777"], ...}
```

```bash
# Linux / macOS
echo '{"server_executable": "/opt/gameserver/GameServer", "port": 7777}' | python3 lifecycle.py validate
```

> **이게 전부입니다.** `validate`와 `get_launch_command`가 올바른 JSON을 반환하면 데몬이 서버를 시작할 수 있습니다.  
> 나머지 기능(설정 동기화, RCON 명령어, 로그 진단 등)은 작동을 확인한 후 하나씩 추가하세요.  
> 각 기능의 상세 스펙은 이어지는 레퍼런스 섹션을 참조하세요.

### 다음 단계: 게임에 맞게 확장하기

최소 모듈이 동작하면, 게임 특성에 따라 아래 순서로 기능을 추가하세요:

| 순서 | 해야 할 일 | 관련 섹션 |
|:----:|-----------|----------|
| 1 | `get_launch_command`의 args를 게임 실제 인자로 수정 | [4.3 필수 함수](#43-필수-함수) |
| 2 | `[settings]`에 서버 설정 필드 추가 (서버명, 최대인원 등) | [3.9 settings](#39-settings--gui-설정-필드) |
| 3 | `configure()`에서 설정을 게임 설정 파일에 쓰기 | [8. 설정 키 매핑](#8-설정-키-매핑-패턴) |
| 4 | `stop()`에 graceful 종료 구현 (RCON/REST) | [9. 프로토콜별 구현](#9-프로토콜별-구현-가이드) |
| 5 | `[commands]`에 GUI 명령어 추가 | [3.10 commands](#310-commands--서버-명령어) |
| 6 | `locales/en.json` 작성 | [5. i18n](#5-i18n-다국어-지원) |
| 7 | `[install]` 설치 방법 정의 | [3.6 install](#36-install--서버-설치-방법) |

---

## 2. 모듈 구조

### 디렉토리 레이아웃

```
mygame/
├── module.toml          # 필수 — 모듈 메타데이터 및 설정 스키마
├── lifecycle.py         # 필수 — 생명주기 관리 로직
├── icon.png             # 권장 — GUI에 표시되는 아이콘
└── locales/             # 권장 — 다국어 번역 파일
    ├── en.json          # 필수 — 영어 (기본 언어)
    ├── ko.json          # 한국어
    ├── ja.json          # 일본어
    ├── de.json          # 독일어
    ├── es.json          # 스페인어
    ├── fr.json          # 프랑스어
    ├── pt-BR.json       # 포르투갈어 (브라질)
    ├── ru.json          # 러시아어
    ├── zh-CN.json       # 중국어 (간체)
    └── zh-TW.json       # 중국어 (번체)
```

### 네이밍 규칙

| 항목 | 규칙 | 예시 |
|------|------|------|
| 폴더명 | 영문 소문자, 하이픈/언더스코어 가능 | `minecraft`, `palworld`, `my_game` |
| `module.name` | 폴더명과 동일 | `"minecraft"` |
| `entry` | 항상 `"lifecycle.py"` | `"lifecycle.py"` |

---

## 3. module.toml 완전 레퍼런스

`module.toml`은 모듈의 모든 메타데이터와 설정 스키마를 정의합니다. 데몬이 이 파일을 파싱하여 `ModuleMetadata`를 생성합니다.

### 3.1 `[module]` — 메타데이터

```toml
[module]
name = "mygame"                         # 고유 식별자 (폴더명과 동일, 영문 소문자)
version = "0.1.0"                       # 모듈 버전 (semver)
description = "MyGame server module"    # 한 줄 설명
game_name = "MyGame"                    # 정식 게임 이름
display_name = "MyGame"                 # GUI에 표시되는 이름
entry = "lifecycle.py"                  # 진입점 스크립트 (항상 lifecycle.py)
icon = "icon.png"                       # 아이콘 파일 (모듈 폴더 내)
log_pattern = '(?P<level>INFO|WARN|ERROR|DEBUG|FATAL)'  # 로그 레벨 추출 정규식
```

#### `log_pattern` 상세

서버 로그에서 로그 레벨을 추출하는 정규식입니다. `(?P<level>...)` 명명된 캡처 그룹으로 레벨을 추출합니다.

**게임별 예시:**

| 게임 | log_pattern | 로그 예시 |
|------|-------------|-----------|
| Minecraft | `'/(?P<level>INFO\|WARN\|ERROR\|DEBUG\|FATAL)\]'` | `[12:00:00] [Server thread/INFO]: Done!` |
| Palworld | `'(?P<level>Info\|Warning\|Error\|Fatal\|Debug)'` | `[2024.01.01-12.00.00][Info] Server started` |
| Zomboid | `'(?P<level>LOG\|WARN\|ERROR\|DEBUG\|FATAL)'` | `LOG  : General, 1234567890> Server Started` |

---

### 3.3 `[protocols]` — 통신 프로토콜

```toml
[protocols]
supported = ["rcon", "stdin"]           # 지원하는 프로토콜 목록
default = "rcon"                        # 기본 사용 프로토콜
interaction_mode = "console"            # GUI 상호작용 방식
```

#### 지원 프로토콜

| 프로토콜 | 설명 | 사용 게임 예시 |
|----------|------|----------------|
| `"rcon"` | Valve Source RCON 프로토콜 | Minecraft, Zomboid |
| `"rest"` | HTTP REST API | Palworld |
| `"stdin"` | 표준 입력 (ManagedProcess의 stdin으로 명령 전송) | Minecraft, Zomboid |

#### `interaction_mode`

| 모드 | 설명 | 사용 게임 |
|------|------|-----------|
| `"console"` | stdin 지원, 실시간 콘솔 UI 표시. 사용자가 자유롭게 명령어 입력 가능 | Minecraft, Zomboid |
| `"commands"` | 정해진 명령어 버튼만 표시. `[commands]`에 정의된 명령어만 실행 | Palworld |

**선택 가이드:**
- 게임 서버가 stdin으로 명령어를 받을 수 있다면 → `"console"` + `supported = ["rcon", "stdin"]`
- REST API만 제공한다면 → `"commands"` + `supported = ["rest"]`

---

### 3.4 `[credential_map]` — 자격증명 동기화

```toml
[credential_map]
rest_password = "AdminPassword"         # 데몬 키 = "게임 설정 키"
# rcon_password = "RCONPassword"
# rest_username = "AdminUser"
```

게임 설정 파일의 자격증명 키와 saba-chan 데몬의 표준 키를 연결합니다. `get_launch_command()`가 게임 키 값을 반환하면, 데몬이 해당 표준 키에 자동 동기화합니다.

#### 사용 가능한 데몬 키

| 데몬 키 | 용도 |
|---------|------|
| `rest_password` | REST API 인증 비밀번호 |
| `rest_username` | REST API 사용자명 |
| `rcon_password` | RCON 인증 비밀번호 |

#### 동작 흐름

```
1. module.toml: [credential_map] rest_password = "AdminPassword"
2. lifecycle.py get_launch_command() 반환값에 "AdminPassword": "abc123" 포함
3. 데몬이 자동으로 instance.rest_password = "abc123" 동기화
4. 이후 REST 명령어 실행 시 자동으로 "abc123" 사용
```

**Palworld 실전 예시:**
Palworld의 `PalWorldSettings.ini`에서 관리자 비밀번호는 `AdminPassword` 키입니다. credential_map을 통해 데몬의 `rest_password`와 자동 동기화됩니다.

---

### 3.5 `[config]` — 기본 실행 설정

```toml
[config]
executable_path = ""                    # 기본 실행 파일 전체 경로 (설치 후 설정)
server_executable = "MyGameServer.exe"  # 실행 파일명 (프로세스 탐지용)
process_name = "MyGameServer"           # 프로세스 이름 (tasklist/pgrep 탐지용)
default_port = 7777                     # 기본 게임 포트
rcon_port = 25575                       # RCON 포트 (RCON 지원 시)
stop_command = "quit"                   # stdin graceful 종료 명령어
```

#### 게임별 비교

| 키 | Minecraft | Palworld | Zomboid |
|----|-----------|----------|---------|
| `server_executable` | `server.jar` | `PalServer.exe` | `StartServer64.bat` |
| `process_name` | `java.exe` | `PalServer-Win64-Shipping-Cmd` | `java.exe` |
| `default_port` | `25565` | `8211` | `16261` |
| `rcon_port` | `25575` | *(없음, REST 사용)* | `27015` |
| `stop_command` | *(없음)* | `Shutdown` | `quit` |

> **참고**: `process_name`은 실제 OS에서 보이는 프로세스 이름입니다.  
> Java 기반 게임(Minecraft, Zomboid)은 모두 `java.exe`로 나타나므로, `[detection].cmd_patterns`로 구분합니다.

> **`stop_command` 동작 방식**: lifecycle.py의 `stop()`이 읽는 값이 **아닙니다**. 데몬이 graceful 종료 시 **ManagedProcess의 stdin에 직접 전송**하는 문자열입니다. (`interaction_mode = "console"`이고 managed 프로세스일 때만 동작합니다.) 커스텀 종료 로직(RCON 전송, 저장 후 종료 등)이 필요하다면 `stop_command`와 관계없이 lifecycle.py에서 `stop()`을 구현하세요.

#### 게임별 추가 키

Minecraft처럼 게임별 전용 키를 추가할 수 있습니다:

```toml
[config]
java_path = "java"                      # Minecraft 전용: Java 경로
server_jar = "server.jar"               # Minecraft 전용: JAR 파일명
eula = true                             # Minecraft 전용: EULA 동의
ram = "2G"                              # Minecraft 전용: 메모리 할당
```

---

### 3.6 `[install]` — 서버 설치 방법

```toml
[install]
method = "steamcmd"                     # 설치 방식
app_id = 123456                         # Steam App ID (steamcmd일 때 필수)
anonymous = true                        # 익명 로그인 가능 여부
install_subdir = "server"               # 설치 하위 폴더
platform = "windows"                    # 대상 플랫폼
requires_extensions = ["steamcmd"]      # 필요한 익스텐션 목록
beta = "unstable"                       # 베타 브랜치 이름 (선택)
```

#### 설치 방식

| method | 설명 | 필수 키 | 사용 게임 |
|--------|------|---------|-----------|
| `"steamcmd"` | SteamCMD로 설치 | `app_id`, `anonymous` | Palworld, Zomboid |
| `"download"` | URL에서 직접 다운로드 | `download_url` | Minecraft |
| `"manual"` | 수동 설치 (안내만 제공) | — | — |

**SteamCMD 방식 (Palworld):**
```toml
[install]
method = "steamcmd"
app_id = 2394010
anonymous = true
install_subdir = "server"
platform = "windows"
requires_extensions = ["steamcmd"]
```

**다운로드 방식 (Minecraft):**
```toml
[install]
method = "download"
download_url = "https://piston-data.mojang.com/v1/objects/{version_hash}/server.jar"
install_subdir = "server"
```

---

### 3.8 `[detection]` — 자동 감지

이미 실행 중이거나 설치되어 있는 서버를 자동으로 찾기 위한 패턴입니다.

```toml
[detection]
process_patterns = ["MyGameServer"]     # 프로세스 이름 패턴 (부분 일치)
cmd_patterns = ["zombie.network"]       # 커맨드라인 패턴 (같은 실행파일 구분용)
common_paths = [                        # 자주 사용되는 설치 경로
    "C:\\Program Files (x86)\\Steam\\steamapps\\common\\MyGame",
]
dir_signatures = ["MyGameServer.exe"]   # 이 파일들이 모두 있으면 해당 게임으로 판정
```

#### 각 필드 상세

**`process_patterns`** — OS 프로세스 목록에서 검색할 패턴입니다.
```toml
# Minecraft: Java 프로세스이므로 여러 패턴을 나열
process_patterns = ["java.exe", "java", "minecraft_server", "server.jar"]

# Palworld: 전용 실행파일명
process_patterns = ["PalServer-Win64-Shipping-Cmd", "PalServer", "PalServer-Win64"]

# Zomboid: Java 기반 + 전용 실행파일
process_patterns = ["ProjectZomboid64", "zombie.network.GameServer", "start-server"]
```

**`cmd_patterns`** — 같은 실행파일(`java.exe`)을 사용하는 게임을 구분하기 위한 커맨드라인 패턴입니다.
```toml
# Minecraft의 java.exe를 Zomboid의 java.exe와 구분
cmd_patterns = ["server.jar", "minecraft_server", "nogui", "net.minecraft"]

# Zomboid의 java.exe를 Minecraft의 java.exe와 구분
cmd_patterns = ["zombie.network.GameServer", "ProjectZomboid", "pzserver", "zomboid"]
```

**`dir_signatures`** — 디렉토리 내 특정 파일의 존재 여부로 게임을 식별합니다. 배열은 AND 조건 (모두 존재해야 매칭).
```toml
# Minecraft: 두 파일이 모두 존재해야 Minecraft 서버로 판정
dir_signatures = ["server.jar", "server.properties"]

# Palworld: 실행파일 하나로 식별
dir_signatures = ["PalServer.exe"]
```

---

### 3.9 `[settings]` — GUI 설정 필드

GUI에 표시되는 인스턴스 설정 필드를 정의합니다. 사용자가 설정한 값은 `config["settings"]`로 lifecycle.py에 전달됩니다.

```toml
[settings]

[[settings.fields]]
name = "server_name"                    # 내부 키 이름 (config에서 참조)
type = "text"                           # 필드 타입
label = "Server Name"                   # 표시 라벨
description = "Name displayed in browser" # 도움말
default = "My Server"                   # 기본값
group = ""                              # 그룹 (비어있으면 일반)
required = false                        # 필수 여부
```

#### 지원 타입

| 타입 | 설명 | 추가 속성 | 예시 |
|------|------|-----------|------|
| `text` | 텍스트 입력 | `placeholder` | 서버 이름, MOTD |
| `number` | 숫자 입력 | `min`, `max`, `step`, `default` | 포트, 최대 플레이어 |
| `boolean` | 온/오프 토글 | `default` | PvP, 화이트리스트 |
| `select` | 드롭다운 선택 | `options`, `default` | 난이도, 게임 모드 |
| `password` | 비밀번호 (마스킹) | `required` | 서버 비밀번호, RCON |
| `file` | 파일 선택 다이얼로그 | `required` | Java 경로 |
| `folder` | 폴더 선택 다이얼로그 | `required` | 설치 경로 |

#### 그룹 (`group`)

| group 값 | 동작 | 용도 |
|----------|------|------|
| `""` (빈 문자열 또는 생략) | 기본 표시 | 일반 게임 설정 |
| `"saba-chan"` | saba-chan 전용 섹션에 표시 | 포트, 메모리, managed_start 등 |
| `"advanced"` | 접힌 상태로 표시 | 고급/세부 설정 |

#### 각 타입 상세 예시

**text:**
```toml
[[settings.fields]]
name = "motd"
type = "text"
label = "MOTD"
description = "Message displayed in the server list"
default = "A Minecraft Server"
```

**number (정수):**
```toml
[[settings.fields]]
name = "max_players"
type = "number"
label = "Max Players"
description = "Maximum number of players (1-100)"
default = 16
min = 1
max = 100
```

**number (소수):**
```toml
[[settings.fields]]
name = "exp_rate"
type = "number"
label = "EXP Rate"
description = "Experience gain multiplier (0.1-20.0)"
default = 1.0
min = 0.1
max = 20.0
step = 0.1
```

**boolean:**
```toml
[[settings.fields]]
name = "pvp"
type = "boolean"
label = "PvP"
description = "Enable player vs player combat"
default = true
```

**select:**
```toml
[[settings.fields]]
name = "difficulty"
type = "select"
label = "Difficulty"
description = "Game difficulty"
options = ["peaceful", "easy", "normal", "hard"]
default = "normal"
```

**password:**
```toml
[[settings.fields]]
name = "server_password"
type = "password"
label = "Server Password"
description = "Password required to join (empty = no password)"
default = ""
required = false
```

**file:**
```toml
[[settings.fields]]
name = "java_path"
type = "file"
label = "Java Path"
description = "Path to Java executable"
default = "java"
required = false
group = "saba-chan"
```

#### 공통 saba-chan 전용 설정

대부분의 모듈에서 다음 설정을 `group = "saba-chan"`으로 정의합니다:

```toml
# 매니지드 시작 모드 (stdin/stdout 콘솔)
[[settings.fields]]
name = "managed_start"
type = "boolean"
label = "Managed Start (Console)"
description = "Start in managed mode with stdin/stdout console"
default = true
group = "saba-chan"

# Graceful 종료
[[settings.fields]]
name = "graceful_stop"
type = "boolean"
label = "Graceful Stop"
description = "Send stop command and wait for graceful shutdown"
default = true
group = "saba-chan"

# 메모리 할당 (Java 기반 게임)
[[settings.fields]]
name = "ram"
type = "number"
label = "Memory (RAM)"
description = "Maximum memory in GB"
default = 2
group = "saba-chan"
```

---

### 3.10 `[commands]` — 서버 명령어

GUI에서 실행할 수 있는 서버 명령어를 정의합니다.

#### RCON 명령어

```toml
[commands]

[[commands.fields]]
name = "say"                            # 명령어 식별자
label = "Send Message"                  # 표시 라벨
description = "Broadcast a message"     # 설명
method = "rcon"                         # 통신 방식: "rcon" 또는 "rest"
rcon_template = "say {message}"         # RCON 명령어 템플릿 ({변수}는 inputs에서 치환)
inputs = [                              # 입력 필드 배열
    { name = "message", type = "text", label = "Message", required = true, placeholder = "Enter message" }
]
```

#### REST 명령어

```toml
[[commands.fields]]
name = "announce"
label = "Send Announcement"
description = "Broadcast a message to the server"
method = "rest"
http_method = "POST"                    # HTTP 메서드
endpoint_template = "/v1/api/announce"  # REST 엔드포인트
inputs = [
    { name = "message", type = "text", label = "Message", required = true, placeholder = "Enter message" }
]
```

#### 입력 없는 명령어

```toml
[[commands.fields]]
name = "save"
label = "Save World"
description = "Save the game world"
method = "rcon"
rcon_template = "save-all"
inputs = []                             # 빈 배열 = 입력 없음
```

#### 명령어 inputs 타입

| 타입 | 설명 | 추가 속성 |
|------|------|-----------|
| `text` | 텍스트 입력 | `placeholder` |
| `number` | 숫자 입력 | `min`, `max`, `default` |
| `select` | 드롭다운 | `options` |
| `password` | 비밀번호 | — |

**복합 입력 예시 (Minecraft give):**
```toml
[[commands.fields]]
name = "give"
label = "Give Item"
description = "Give an item to a player"
method = "rcon"
rcon_template = "give {player} {item} {amount}"
inputs = [
    { name = "player", type = "text", label = "Player Name", required = true, placeholder = "Player nickname" },
    { name = "item", type = "text", label = "Item ID", required = true, placeholder = "e.g. diamond" },
    { name = "amount", type = "number", label = "Amount", required = true, default = 1, min = 1, max = 64 }
]
```

#### RCON vs REST 선택

| 상황 | 사용할 method |
|------|---------------|
| 게임이 RCON을 지원 | `"rcon"` + `rcon_template` |
| 게임이 REST API를 제공 | `"rest"` + `http_method` + `endpoint_template` |
| 커스텀 로직 필요 | `method` 생략, lifecycle.py의 `command()`에서 처리 |

> **참고**: `method = "rcon"` + `rcon_template`이 정의된 명령어는 데몬이 직접 RCON으로 전송합니다. lifecycle.py의 `command()`에는 도달하지 않습니다. 커스텀 로직이 필요하면 rcon_template을 생략하세요.

---

### 3.11 `[errors]` — 에러 메시지

기본 에러 메시지를 정의합니다. i18n 번역이 없을 때 폴백으로 사용됩니다.

```toml
[errors]
server_not_running = "Server is not running. Please start the server first"
auth_failed = "RCON authentication failed: please check the password"
connection_refused = "Cannot connect to server. Please check port and settings"
timeout = "Server response timeout. Please check server status"
internal_server_error = "Internal server error occurred"
```

---

### 3.12 `[aliases]` — 별칭

Discord 봇 등에서 사용할 수 있는 다국어 별칭입니다.

```toml
[aliases]
module_aliases = ["마인크래프트", "마크", "mc", "minecraft"]

[aliases.commands]
start = { aliases = ["시작", "실행", "켜기"], description = "Start the server" }
stop = { aliases = ["정지", "끄기"], description = "Stop the server" }
status = { aliases = ["상태", "확인"], description = "Check server status" }
say = { aliases = ["말하기", "채팅"], description = "Send a message" }
```

> 각 명령어의 `aliases`에는 한국어, 영어 등 다양한 언어의 별칭을 넣을 수 있습니다.

---

### 3.13 `[syntax_highlight]` — 콘솔 구문 강조

`interaction_mode = "console"`일 때 적용되는 콘솔 출력 하이라이팅 규칙입니다.

```toml
[syntax_highlight]

[[syntax_highlight.rules]]
name = "level_error"                    # 규칙 이름 (고유)
pattern = '(?P<hl>ERROR|FATAL):'        # 정규식 패턴
color = "error"                         # 색상 (시맨틱 토큰 또는 CSS)
bold = true                             # 굵게 (선택)
italic = false                          # 이탈릭 (선택)
```

#### 정규식 패턴 규칙

- `(?P<hl>...)` — 명명된 캡처 그룹이 있으면 해당 부분만 색상 적용
- 캡처 그룹이 없으면 전체 매치에 색상 적용
- 규칙 적용 순서 = 선언 순서 (먼저 매치된 범위가 우선)

#### 시맨틱 컬러 토큰

| 토큰 | 용도 | 예시 |
|------|------|------|
| `"info"` | 정보 로그 | INFO 레벨 |
| `"warn"` | 경고 | WARN 레벨 |
| `"error"` | 에러 | ERROR, FATAL |
| `"debug"` | 디버그 | DEBUG 레벨 |
| `"dim"` | 흐릿한 텍스트 | INFO 레벨 (Minecraft) |
| `"success"` | 성공 메시지 | "Done!", "Server Started" |
| `"timestamp"` | 타임스탬프 | `[12:00:00]` |
| `"player"` | 플레이어 이름 | 접속/퇴장 메시지의 닉네임 |
| `"ip"` | IP 주소 | `192.168.1.1:25565` |
| `"uuid"` | UUID / SteamID | `76561198012345678` |
| `"number"` | 숫자/좌표 | `100, 64, -200` |
| `"path"` | 파일 경로 | `C:\Server\...` |
| `"keyword"` | 키워드/네임스페이스 | `minecraft:diamond` |
| `"command"` | 명령어 | `/tp`, `/give` |
| `"selector"` | 셀렉터 | `@p`, `@a[r=10]` |
| `"string"` | 문자열 리터럴 | `"quoted text"` |

CSS 색상 코드도 사용 가능: `"#a6e3a1"`, `"#f38ba8"` 등.

#### Minecraft 하이라이팅 핵심 규칙 예시

```toml
# 타임스탬프
[[syntax_highlight.rules]]
name = "timestamp"
pattern = '^\[\d{2}:\d{2}:\d{2}\]'
color = "timestamp"

# Done 메시지 (서버 시작 완료)
[[syntax_highlight.rules]]
name = "done_message"
pattern = '(?P<hl>Done \([^)]+\)!.*)'
color = "success"
bold = true

# 플레이어 접속
[[syntax_highlight.rules]]
name = "player_join"
pattern = '(?P<hl>\w{3,16}) joined the game'
color = "#a6e3a1"
bold = true

# 사망 메시지
[[syntax_highlight.rules]]
name = "death_message"
pattern = ': (?P<hl>\w{3,16} (was (slain|shot|blown|killed)|drowned|burned|fell).*)'
color = "#f38ba8"

# Java 스택트레이스
[[syntax_highlight.rules]]
name = "stacktrace_at"
pattern = '^\s+at .*'
color = "#585b70"
```

---

## 4. lifecycle.py 완전 레퍼런스

### 4.1 기본 구조

```python
#!/usr/bin/env python3
"""모듈 설명"""

import sys
import json
import subprocess
import os
import time
from i18n import I18n

# ─── Path setup ──────────────────────────────────────────
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

# daemon_rcon.py, i18n.py는 saba-chan 본체(modules/)에 있습니다.
# 데몬이 실행 시 PYTHONPATH로 해당 경로를 자동 주입하므로,
# 모듈 폴더에 복사할 필요 없이 바로 import 가능합니다.
from daemon_rcon import rcon_command as _rcon_command_bridge
from i18n import I18n

# i18n 초기화
i18n = I18n(MODULE_DIR)

# 데몬 API URL (환경 변수로 주입됨)
DAEMON_API_URL = os.environ.get('DAEMON_API_URL', 'http://127.0.0.1:57474')


# ═══ 설정 키 매핑 ═══
_KEY_MAP = {
    "server_name": "ServerName",
    # saba-chan 키 → 게임 설정 파일 키
}


# ═══ 생명주기 함수들 ═══
def validate(config):
    ...

def get_launch_command(config):
    ...

def stop(config):
    ...

def status(config):
    ...

def configure(config):
    ...

def import_settings(config):
    ...

def command(config):
    ...


# ═══ CLI 진입점 ═══
FUNCTIONS = {
    "validate": validate,
    "get_launch_command": get_launch_command,
    "stop": stop,
    "status": status,
    "configure": configure,
    "import_settings": import_settings,
    "command": command,
}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "message": "Usage: lifecycle.py <function>"}))
        sys.exit(1)

    function_name = sys.argv[1]

    # config JSON을 stdin에서 읽기
    try:
        config_str = sys.stdin.read()
        config = json.loads(config_str) if config_str.strip() else {}
    except json.JSONDecodeError:
        print(json.dumps({"success": False, "message": "Invalid JSON config on stdin"}))
        sys.exit(1)

    fn = FUNCTIONS.get(function_name)
    if fn:
        result = fn(config)
    else:
        result = {"success": False, "message": f"Unknown function: {function_name}"}

    print(json.dumps(result, ensure_ascii=False))
```

---

### 4.2 config 객체

데몬이 전달하는 config dict에는 다음 키들이 포함됩니다:

#### 항상 포함되는 키

| 키 | 타입 | 설명 |
|----|------|------|
| `instance_id` | `str` | 인스턴스 UUID |
| `server_executable` | `str` | 실행 파일 경로 |
| `working_dir` | `str` | 작업 디렉토리 |
| `port` | `int` | 게임 포트 |
| `settings` | `dict` | 사용자 설정 (`module.toml`의 `[[settings.fields]]`에서 정의) |

#### RCON 관련 키

| 키 | 타입 | 설명 |
|----|------|------|
| `rcon_port` | `int` | RCON 포트 |
| `rcon_password` | `str` | RCON 비밀번호 (자동 생성) |

#### REST 관련 키

| 키 | 타입 | 설명 |
|----|------|------|
| `rest_host` | `str` | REST API 호스트 |
| `rest_port` | `int` | REST API 포트 |
| `rest_username` | `str` | REST API 사용자명 (항상 `"admin"`) |
| `rest_password` | `str` | REST API 비밀번호 (자동 생성) |

#### 함수별 추가 키

| 함수 | 추가 키 | 설명 |
|------|---------|------|
| `stop` | `pid`, `force`, `graceful_stop` | 프로세스 PID, 강제 종료 플래그 |
| `command` | `command`, `args` | 명령어 이름, 인자 dict |
| `install_server` | `install_dir`, `version` | 설치 디렉토리, 설치할 버전 |
| `status` | `pid` | 프로세스 PID |

---

### 4.3 필수 함수

데몬이 호출하는 핵심 함수들입니다. 모든 함수는 반드시 `dict`를 반환해야 합니다.

#### `validate(config)` — 사전 검증

서버를 시작하기 전 환경을 검증합니다.

```python
def validate(config):
    issues = []

    # 1) 실행 파일 존재 여부
    executable = config.get("server_executable", "")
    if not executable or not os.path.exists(executable):
        issues.append({
            "code": "EXECUTABLE_NOT_FOUND",    # 에러 코드
            "severity": "critical",             # "critical" 또는 "warning"
            "message": "Server executable not found",
            "solution": "Install the server first."
        })

    # 2) 작업 디렉토리
    working_dir = config.get("working_dir", "")
    if working_dir and not os.path.isdir(working_dir):
        issues.append({
            "code": "WORKING_DIR_MISSING",
            "severity": "warning",              # warning = 시작은 가능
            "message": f"Working directory not found: {working_dir}",
            "solution": "Directory will be created on start."
        })

    has_critical = any(i["severity"] == "critical" for i in issues)
    return {
        "success": not has_critical,
        "issues": issues
    }
```

**issue 객체 구조:**

| 필드 | 타입 | 설명 |
|------|------|------|
| `code` | `str` | 고유 에러 코드 (대문자 스네이크) |
| `severity` | `str` | `"critical"` (시작 불가) 또는 `"warning"` (경고만) |
| `message` | `str` | 사용자에게 표시할 메시지 |
| `solution` | `str` | 해결 방법 안내 |

**Minecraft validate 실전 예시:**
- Java 존재 여부 및 버전 검사
- server.jar 존재 여부
- EULA 동의 여부
- 포트 사용 여부

---

#### `get_launch_command(config)` ★ 가장 중요한 함수

데몬의 ManagedProcess가 서버를 실행하기 위한 정보를 반환합니다.

```python
def get_launch_command(config):
    executable = config.get("server_executable")
    if not executable or not os.path.exists(executable):
        return {"success": False, "message": "Executable not found"}

    port = config.get("port", 7777)
    working_dir = config.get("working_dir") or os.path.dirname(executable)

    # 실행 전 준비 작업 (게임별)
    # 예: 설정 파일 동기화, RCON 정책 적용 등

    return {
        "success": True,
        "program": os.path.abspath(executable),     # 필수: 실행 파일 절대 경로
        "args": [f"--port={port}"],                  # 필수: 커맨드라인 인자
        "working_dir": os.path.abspath(working_dir), # 필수: 작업 디렉토리
        "env_vars": {},                              # 필수: 환경 변수 (빈 dict 가능)
    }
```

**필수 반환 키:**

| 키 | 타입 | 설명 |
|----|------|------|
| `success` | `bool` | 성공 여부 |
| `program` | `str` | 실행 파일 절대 경로 |
| `args` | `list[str]` | 커맨드라인 인자 |
| `working_dir` | `str` | 작업 디렉토리 절대 경로 |
| `env_vars` | `dict` | 환경 변수 |

**실패 시 선택 키:**

| 키 | 타입 | 설명 |
|----|------|------|
| `action_required` | `str` | 사용자 액션 필요 유형 |
| `options` | `list[str]` | 사용자에게 제시할 선택지 |
| `configured_path` | `str` | 현재 설정된 경로 |

**credential_map 연동:**

`module.toml`에 `[credential_map]`이 선언되어 있으면, get_launch_command의 반환값에 게임 키 값을 포함시켜야 합니다:

```python
# module.toml: [credential_map] rest_password = "AdminPassword"
# → 반환값에 "AdminPassword" 키로 실제 비밀번호를 포함

return {
    "success": True,
    "program": ...,
    "args": ...,
    "working_dir": ...,
    "env_vars": {},
    "AdminPassword": actual_password,  # ← 데몬이 rest_password에 자동 동기화
}
```

**게임별 get_launch_command 비교:**

| 게임 | program | args (핵심) |
|------|---------|-------------|
| Minecraft | `java` (또는 번들 JRE) | `["-Xmx2G", "-jar", "server.jar", "nogui"]` |
| Palworld | `PalServer.exe` | `["-port=8211", "-RESTAPIEnabled=True"]` |
| Zomboid | `java.exe` (번들 JRE) | `["-Xmx4G", "-cp", "...classpath...", "zombie.network.GameServer"]` |

---

#### `stop(config)` — 서버 종료

```python
def stop(config):
    pid = config.get("pid")
    force = config.get("force", False)
    graceful = config.get("graceful_stop", True) and not force

    if graceful and pid:
        # 1) Graceful: RCON/REST로 종료 명령 전송
        rcon_password = config.get("rcon_password", "")
        rcon_port = config.get("rcon_port", 25575)
        if rcon_password:
            try:
                _rcon_command_bridge("127.0.0.1", rcon_port, rcon_password, "stop")
                # 프로세스 종료 대기 (최대 30초)
                for _ in range(30):
                    if not _is_process_running(pid):
                        return {"success": True, "message": "Server stopped gracefully"}
                    time.sleep(1)
            except Exception as e:
                print(f"Graceful stop failed: {e}", file=sys.stderr)

    # 2) Force kill
    if pid:
        _force_kill(pid)
        return {"success": True, "message": "Server force-killed"}

    return {"success": False, "message": "No PID available"}
```

**종료 전략 (우선순위):**
1. **Graceful**: RCON/REST/stdin으로 종료 명령 전송 → 프로세스 종료 대기
2. **Force**: 즉시 `taskkill`/`kill` (graceful 실패 시 또는 `force=True`)

**Zomboid 특수 케이스:**
Zomboid는 종료 전 `save` → `quit` 순서로 두 명령어를 전송합니다.

---

#### `status(config)` — 상태 조회

```python
def status(config):
    pid = config.get("pid")

    if pid and _is_process_running(pid):
        return {
            "success": True,
            "status": "running",
            "players_online": 5,       # 선택: 접속자 수
            "players_max": 20,         # 선택: 최대 접속자
        }

    return {"success": True, "status": "stopped"}
```

**status 값:**

| 값 | 의미 |
|----|------|
| `"running"` | 정상 동작 중 |
| `"stopped"` | 정지됨 |
| `"starting"` | 시작 중 (아직 응답 없음) |

**Minecraft 강화 상태 조회:**

Minecraft는 Server List Ping (SLP) 프로토콜로 상세 정보를 조회합니다:

```python
return {
    "success": True,
    "status": "running",
    "online": True,
    "players_online": 5,
    "players_max": 20,
    "player_list": ["Player1", "Player2"],
    "version": "1.21.11",
    "motd": "A Minecraft Server",
}
```

---

#### `configure(config)` — 설정 적용

사용자 설정을 게임 설정 파일에 적용합니다.

```python
def configure(config):
    settings = config.get("settings", {})
    if not settings:
        return {"success": True, "message": "No settings to apply"}

    working_dir = config.get("working_dir", "")
    config_file = os.path.join(working_dir, "server.cfg")

    try:
        current = _read_config_file(config_file)
        
        for saba_key, value in settings.items():
            game_key = _KEY_MAP.get(saba_key)
            if game_key:
                current[game_key] = value

        _write_config_file(config_file, current)
        return {"success": True, "message": f"Applied {len(settings)} settings"}
    except Exception as e:
        return {"success": False, "message": str(e)}
```

**키 매핑 패턴:**
- `_KEY_MAP`으로 saba-chan 내부 키 → 게임 설정 파일 키를 변환
- 자세한 내용은 [8. 설정 키 매핑 패턴](#8-설정-키-매핑-패턴) 참조

---

#### `import_settings(config)` — 설정 가져오기

기존 게임 설정 파일을 읽어 saba-chan 키로 변환합니다 (역매핑).

```python
def import_settings(config):
    working_dir = config.get("working_dir", "")
    config_file = os.path.join(working_dir, "server.cfg")

    if not os.path.exists(config_file):
        return {"success": False, "message": "Config file not found"}

    try:
        current = _read_config_file(config_file)
        reverse_map = {v: k for k, v in _KEY_MAP.items()}
        
        imported = {}
        for game_key, value in current.items():
            saba_key = reverse_map.get(game_key)
            if saba_key:
                imported[saba_key] = value

        return {"success": True, "settings": imported}
    except Exception as e:
        return {"success": False, "message": str(e)}
```

> 첫 연결 시 기존 설정을 자동 임포트하는 데 사용됩니다.

---

#### `command(config)` — 명령어 실행

```python
def command(config):
    cmd = config.get("command", "")
    args = config.get("args", {})
    instance_id = config.get("instance_id", "")

    # 커스텀 명령어 처리
    if cmd == "backup":
        return _do_backup(config)

    # 기본: RCON으로 전달
    rcon_password = config.get("rcon_password", "")
    rcon_port = config.get("rcon_port", 25575)

    try:
        response = _rcon_command_bridge(
            "127.0.0.1", rcon_port, rcon_password, cmd
        )
        return {"success": True, "response": response}
    except Exception as e:
        return {"success": False, "message": str(e)}
```

> **참고**: `module.toml`에서 `method = "rcon"` + `rcon_template`이 정의된 명령어는 데몬이 직접 처리합니다. lifecycle.py의 `command()`에는 **rcon_template이 없는 커스텀 명령어**만 도달합니다.

---

### 4.4 선택 함수

게임에 따라 필요한 경우에만 구현합니다.

#### 시작은 `get_launch_command(config)` 하나로

> 데몬은 managed·non-managed 두 시작 모드 모두 `get_launch_command()`를 불러 명령을 받고, 프로세스는 데몬(saba-runner)이 직접 띄웁니다.
> 모듈이 `start()`로 프로세스를 띄우는 방식은 쓰지 않습니다. 모드 차이는 `config["managed"]`로 구분합니다(예: RCON 정책).

---

#### `install_server(config)` — 서버 설치

```python
def install_server(config):
    install_dir = config.get("install_dir", "")
    version = config.get("version", "")

    if not install_dir:
        return {"success": False, "message": "install_dir not specified"}

    # 직접 받는 예시 (method = "download"). SteamCMD 게임은 [install] method = "steamcmd"만 적으면
    # steamcmd 익스텐션이 설치를 맡으므로 install_server()를 만들 필요가 없다
    url = f"https://example.com/server-{version}.zip"
    ...
    return {"success": True, "install_dir": install_dir}
```

**Minecraft install_server 특징:**
- Mojang API에서 버전 매니페스트 다운로드
- SHA1 검증
- EULA 자동 동의 (옵션)
- Java 자동 다운로드 (Adoptium)

---

#### `accept_eula(config)` — EULA 동의

```python
def accept_eula(config):
    working_dir = config.get("working_dir", "")
    eula_path = os.path.join(working_dir, "eula.txt")
    
    with open(eula_path, "w") as f:
        f.write("eula=true\n")
    
    return {"success": True, "message": "EULA accepted"}
```

---

#### `diagnose_log(config)` — 로그 진단

```python
def diagnose_log(config):
    log_lines = config.get("log_lines", [])
    working_dir = config.get("working_dir", "")

    # 로그 파일에서 읽기 (log_lines가 없을 때)
    if not log_lines and working_dir:
        log_path = os.path.join(working_dir, "logs", "latest.log")
        if os.path.isfile(log_path):
            with open(log_path, "r", errors="replace") as f:
                log_lines = f.readlines()[-500:]

    issues = []
    # 패턴 매칭으로 에러 진단
    for line in log_lines:
        if "OutOfMemoryError" in line:
            issues.append({
                "code": "OUT_OF_MEMORY",
                "severity": "critical",
                "matched_line": line.strip(),
                "message": "Server ran out of memory",
                "solution": "Increase RAM allocation in settings"
            })

    return {"success": True, "issues": issues, "lines_analyzed": len(log_lines)}
```

---

#### `list_versions(config)` — 버전 목록

```python
def list_versions(config):
    page = config.get("page", 1)
    per_page = config.get("per_page", 25)

    return {
        "success": True,
        "versions": [
            {"id": "1.21.11", "type": "release", "release_time": "2024-12-01"},
            {"id": "1.21.10", "type": "release", "release_time": "2024-11-01"},
        ],
        "latest": {"release": "1.21.11"},
        "total": 50,
        "page": page,
        "per_page": per_page,
        "total_pages": 2,
    }
```

---

#### `get_version_details(config)` — 버전 상세 정보

```python
def get_version_details(config):
    version_id = config.get("version")

    return {
        "success": True,
        "id": version_id,
        "has_server": True,
        "download_url": "https://...",
        "sha1": "abc123...",
        "size": 50000000,
        "java_major_version": 21,
    }
```

---

#### `read_properties(config)` — 설정 파일 읽기

```python
def read_properties(config):
    working_dir = config.get("working_dir")
    # 게임 설정 파일을 파싱하여 raw 키-값 반환
    return {"success": True, "exists": True, "properties": {...}}
```

---

#### `get_credentials(config)` — 자격증명 조회

게임 설정 파일에서 직접 자격증명(RCON 비밀번호, 포트 등)을 읽어 반환합니다. 데몬이 명령어 실행 직전에 호출하며, 설정 파일이 외부에서 변경된 경우에도 최신 값을 사용할 수 있습니다.

게임 설정 파일에서 RCON/REST 비밀번호를 직접 관리하는 모듈(Minecraft 등)에서 구현합니다.

```python
def get_credentials(config):
    working_dir = config.get("working_dir", "")
    if not working_dir:
        return {"success": False, "message": "working_dir not specified"}

    # 게임별 설정 파일에서 자격증명 읽기
    # (Minecraft 예시: server.properties)
    props = _read_config_file(os.path.join(working_dir, "server.cfg"))
    return {
        "success": True,
        "rcon_password": props.get("rcon.password", ""),
        "rcon_port": int(props.get("rcon.port", "25575")),
    }
```

**반환 키:**

| 키 | 타입 | 설명 |
|----|------|------|
| `rcon_password` | `str` | RCON 비밀번호 |
| `rcon_port` | `int` | RCON 포트 |
| `rest_password` | `str` | REST API 비밀번호 (REST 게임의 경우) |

> **참고**: `get_credentials`를 구현하면 데몬이 FUNCTIONS dict에 등록된 것을 감지하여 명령어 실행 전에 자동 호출합니다. credential_map을 사용하는 경우에는 대부분 불필요합니다.

---

#### `reset_properties(config)` — 설정 초기화

```python
def reset_properties(config):
    # 설정 파일을 기본값으로 덮어쓰기
    return {"success": True, "message": "Settings reset to defaults"}
```

---

#### `reset_server(config)` — 서버 전체 리셋

```python
def reset_server(config):
    # 서버 파일 삭제 (실행파일, EULA 보존)
    return {"success": True, "deleted": ["world/", "logs/", "server.properties"]}
```

---

### 4.5 반환값 규칙

모든 함수는 반드시 `dict`를 반환해야 합니다. 이 dict는 JSON으로 직렬화되어 stdout에 출력됩니다.

#### 공통 키

| 키 | 타입 | 필수 | 설명 |
|----|------|------|------|
| `success` | `bool` | ✅ | 작업 성공 여부 |
| `message` | `str` | 권장 | 결과 메시지 |

#### 로깅 규칙

```python
# ✅ 올바른 로깅 (stderr 사용)
print("Starting server...", file=sys.stderr)
print(f"[DEBUG] Config: {config}", file=sys.stderr)

# ❌ 잘못된 로깅 (stdout 사용 — JSON 파싱 오류 발생!)
print("Starting server...")
```

---

## 5. i18n (다국어 지원)

### 5.1 설정

모듈 폴더에 `locales/` 디렉토리를 만들고 언어별 JSON 파일을 배치합니다.

```
mygame/
└── locales/
    ├── en.json     # 기본 (필수)
    ├── ko.json
    └── ja.json
```

### 5.2 번역 파일 형식

```json
{
  "module": {
    "display_name": "MyGame",
    "description": "MyGame server management module"
  },
  "errors": {
    "server_not_found": "Server executable not found: {{path}}",
    "detect": {
      "java_not_found": "Java not found",
      "port_in_use": "Port {{port}} is already in use"
    },
    "solutions": {
      "java_not_found": "Install Java 17 or higher",
      "port_in_use": "Change the port or stop the other process using port {{port}}"
    }
  },
  "messages": {
    "server_starting": "Server starting (PID: {{pid}})",
    "graceful_stop": "Server stopped gracefully (PID: {{pid}})"
  },
  "rcon": {
    "connecting": "Connecting to RCON at {{host}}:{{port}}",
    "authenticated": "RCON authenticated successfully"
  }
}
```

### 5.3 사용법

```python
from i18n import I18n

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
i18n = I18n(MODULE_DIR)

# 단순 번역
msg = i18n.t("errors.server_not_found")

# 변수 보간 ({{변수명}} → 값)
msg = i18n.t("errors.detect.port_in_use", port="25565")

# 중첩 키 (도트 표기법)
msg = i18n.t("errors.solutions.java_not_found")
```

### 5.4 언어 결정

1. 환경 변수 `SABA_LANG`에서 현재 언어 결정 (데몬이 주입)
2. 현재 언어에 번역이 없으면 → `en.json` 폴백
3. `en.json`에도 없으면 → 키 문자열 자체를 반환

---

## 6. 데몬 API 연동

### 6.1 `daemon_rcon` — RCON 브릿지

saba-chan 데몬에는 내장 RCON 클라이언트가 있습니다. 모듈은 직접 TCP 소켓을 열지 않고, 데몬의 HTTP API를 통해 RCON 명령을 실행합니다.

#### 원-샷 호출

```python
from daemon_rcon import rcon_command

# 단순 호출 시그니처 (host, port, password는 데몬이 관리)
response = rcon_command("127.0.0.1", 25575, "password", "list")
```

#### 세션 기반 호출

```python
from daemon_rcon import DaemonRconClient

client = DaemonRconClient(instance_id="...", port=25575, password="...")
client.connect()          # no-op (데몬이 연결 관리)
response = client.command("save")
response2 = client.command("quit")
client.disconnect()       # no-op
```

#### instance_id 기반 호출

```python
from daemon_rcon import daemon_rcon_command

# 권장: instance_id로 호출 (데몬이 포트/비밀번호를 자동 조회)
response = daemon_rcon_command(instance_id, "list")
```

### 6.2 환경 변수

데몬이 lifecycle.py 실행 시 자동으로 주입하는 환경 변수:

| 변수 | 설명 | 기본값 |
|------|------|--------|
| `DAEMON_API_URL` | 데몬 HTTP API URL | `http://127.0.0.1:57474` |
| `SABA_LANG` | 현재 언어 코드 | `en` |
| `PYTHONPATH` | saba-chan 본체의 `modules/` 디렉토리 | 자동 주입 |

---

## 7. 익스텐션 활용

saba-chan 익스텐션은 모듈 간에 공유되는 유틸리티입니다.

### SteamCMD 익스텐션

설치·업데이트는 `module.toml`에 선언만 하면 익스텐션이 맡습니다. 모듈 코드에서 부를 일은 거의 없습니다.

```toml
[install]
method = "steamcmd"          # 이 이름의 익스텐션이 설치·업데이트를 맡는다
app_id = 2394010
anonymous = true
requires_extensions = ["steamcmd"]
```

직접 써야 하면 `SteamCMD` 클래스를 씁니다:

```python
from extensions.steamcmd import SteamCMD

SteamCMD().install(app_id=2394010, install_dir="/path/to/server", anonymous=True)
```

### UE4 INI 파서 익스텐션

Palworld 등 UE4 기반 게임의 INI 파일을 파싱합니다:

```python
from extensions.ue4_ini import parse_option_settings, write_option_settings

# INI에서 OptionSettings 파싱
settings = parse_option_settings(ini_content)

# 설정값을 INI에 쓰기
new_content = write_option_settings(ini_content, settings)
```

---

## 8. 설정 키 매핑 패턴

모듈의 핵심 패턴 중 하나는 saba-chan 내부 키와 게임 설정 파일 키 간의 변환입니다.

### 매핑 테이블

```python
_KEY_MAP = {
    # saba-chan 키 (snake_case)    # 게임 설정 파일 키
    "server_name":                 "ServerName",
    "max_players":                 "MaxPlayers",
    "port":                        "ServerPort",
}
```

### 게임별 설정 파일 형식

| 게임 | 설정 파일 | 형식 |
|------|-----------|------|
| Minecraft | `server.properties` | Java Properties (key=value) |
| Palworld | `PalWorldSettings.ini` | UE4 INI (OptionSettings=(...)) |
| Zomboid | `servertest.ini` | INI (Key=Value) |

### Minecraft 매핑 예시

```python
_PROPERTY_KEY_MAP = {
    "allow_flight":     "allow-flight",      # 언더스코어 → 하이픈
    "max_players":      "max-players",
    "port":             "server-port",       # 이름 변환
    "rcon_port":        "rcon.port",         # 도트 키
    "rcon_password":    "rcon.password",
}
```

### Zomboid 매핑 예시

```python
_INI_KEY_MAP = {
    "public_name":      "PublicName",        # 스네이크 → 파스칼
    "max_players":      "MaxPlayers",
    "port":             "DefaultPort",       # 이름 변환
    "rcon_port":        "RCONPort",
    "rcon_password":    "RCONPassword",
}
```

### 역매핑 (import_settings)

```python
# 게임 키 → saba-chan 키 역변환
reverse_map = {v: k for k, v in _KEY_MAP.items()}
```

---

## 9. 프로토콜별 구현 가이드

### 9.1 RCON 모듈 (Minecraft, Zomboid)

RCON을 지원하는 게임의 전형적인 패턴입니다.

```toml
# module.toml
[protocols]
supported = ["rcon", "stdin"]
default = "rcon"
interaction_mode = "console"

[config]
rcon_port = 25575
stop_command = "stop"
```

```python
# lifecycle.py
from daemon_rcon import rcon_command as _rcon_command_bridge

def stop(config):
    pid = config.get("pid")
    rcon_password = config.get("rcon_password", "")
    rcon_port = config.get("rcon_port", 25575)

    if rcon_password:
        _rcon_command_bridge("127.0.0.1", rcon_port, rcon_password, "stop")
        # 종료 대기...
```

#### Managed 모드와 RCON 정책

- **Managed 모드 (stdin)**: RCON을 비활성화하고 stdin으로 제어
- **Non-managed 모드**: RCON을 활성화하고 RCON으로 제어

```python
def _enforce_rcon_policy(working_dir, managed=True):
    if managed:
        # RCON OFF (stdin으로 제어)
        update_config({"enable-rcon": "false"})
    else:
        # RCON ON + 비밀번호 자동 생성
        update_config({"enable-rcon": "true", "rcon.password": generate_password()})
```

---

### 9.2 REST API 모듈 (Palworld)

REST API를 지원하는 게임의 패턴입니다.

```toml
# module.toml
[protocols]
supported = ["rest"]
default = "rest"
interaction_mode = "commands"

[credential_map]
rest_password = "AdminPassword"

[config]
default_port = 8211
```

```python
# lifecycle.py
import urllib.request, urllib.error, base64, json

class RestClient:
    def __init__(self, host, port, username, password):
        self.base_url = f"http://{host}:{port}/v1/api"
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.auth = f"Basic {token}"

    def _request(self, method, path, payload=None):
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json", "Authorization": self.auth}
        data = json.dumps(payload).encode() if payload else None
        if data:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode()
            return json.loads(body) if body else None

def status(config):
    rest_host = config.get("rest_host", "127.0.0.1")
    rest_port = config.get("rest_port", 8212)
    rest_password = config.get("rest_password", "")

    client = RestClient(rest_host, rest_port, "admin", rest_password)
    try:
        info = client._request("GET", "/info")
        players = client._request("GET", "/players")
        return {
            "success": True,
            "status": "running",
            "players_online": len(players.get("players", [])),
        }
    except Exception:
        return {"success": True, "status": "stopped"}
```

---

### 9.3 stdin 모듈 (콘솔 모드)

stdin을 통해 명령어를 받는 게임의 패턴입니다. `interaction_mode = "console"`에서 사용합니다.

```toml
# module.toml
[protocols]
supported = ["rcon", "stdin"]
default = "rcon"
interaction_mode = "console"

[config]
stop_command = "quit"       # stdin으로 전송하는 종료 명령어
```

데몬이 ManagedProcess의 stdin을 통해 직접 명령어를 전송합니다. lifecycle.py에서 특별한 처리가 필요하지 않습니다.

---

## 10. 설치 방식별 구현

### SteamCMD 설치 (Palworld, Zomboid)

`[install] method = "steamcmd"`와 `app_id`·`anonymous`(선택 `beta`)만 선언합니다. 설치·업데이트는
steamcmd 익스텐션이 하고, 모듈은 `install_server()`를 만들지 않습니다.

### 다운로드 설치 (Minecraft)

```python
def install_server(config):
    version_id = config.get("version", "1.21.11")
    install_dir = config.get("install_dir", "")

    # 1. 버전 매니페스트에서 다운로드 URL 조회
    details = get_version_details(version_id)
    download_url = details["download_url"]
    expected_sha1 = details["sha1"]

    # 2. 다운로드 + SHA1 검증
    jar_path = os.path.join(install_dir, "server.jar")
    download_with_progress(download_url, jar_path)
    verify_sha1(jar_path, expected_sha1)

    # 3. 후처리 (EULA, 초기 설정, Java 다운로드)
    if config.get("accept_eula"):
        accept_eula(config)

    java_req = details.get("java_major_version", 21)
    ensure_java(java_req, install_dir)

    return {"success": True, "install_path": install_dir}
```

---

## 11. 에러 진단 시스템

Minecraft 모듈의 에러 진단 시스템을 참고한 패턴입니다.

### 에러 패턴 정의

```python
ERROR_PATTERNS = [
    {
        "code": "JAVA_NOT_FOUND",
        "patterns": [
            r"'java' is not recognized",
            r"java: not found",
        ],
        "severity": "critical",
    },
    {
        "code": "PORT_IN_USE",
        "patterns": [
            r"Address already in use",
            r"FAILED TO BIND TO PORT",
        ],
        "severity": "critical",
    },
    {
        "code": "OUT_OF_MEMORY",
        "patterns": [
            r"java\.lang\.OutOfMemoryError",
            r"Could not reserve enough space",
        ],
        "severity": "critical",
    },
    {
        "code": "SERVER_OVERLOADED",
        "patterns": [
            r"Can't keep up! Is the server overloaded\?",
        ],
        "severity": "warning",
    },
]
```

### 진단 엔진

```python
class ErrorDetector:
    @staticmethod
    def diagnose(log_lines):
        issues = []
        seen_codes = set()

        for line in log_lines:
            for error_def in ERROR_PATTERNS:
                if error_def["code"] in seen_codes:
                    continue
                for regex in error_def["patterns"]:
                    if re.search(regex, line, re.IGNORECASE):
                        seen_codes.add(error_def["code"])
                        issues.append({
                            "code": error_def["code"],
                            "severity": error_def["severity"],
                            "matched_line": line.strip(),
                            "message": i18n.t(f"errors.detect.{error_def['code'].lower()}"),
                            "solution": i18n.t(f"errors.solutions.{error_def['code'].lower()}"),
                        })
                        break
        return issues
```

---

## 12. 로컬 테스트 & 디버깅

데몬을 실행하지 않고도 모듈의 각 함수를 직접 테스트할 수 있습니다.

### 적합성 키트

PR을 올리기 전에 [적합성 키트](../conformance/README.md)를 돌리세요. CI도 같은 검사를 합니다.

```bash
pytest conformance --module path/to/mygame --shared-libs path/to/saba-chan/modules
```

### 터미널에서 함수별 테스트

lifecycle.py는 일반 Python 스크립트이므로, JSON을 stdin으로 넘기면 바로 실행됩니다:

```powershell
# ── validate 테스트 ──
echo '{"server_executable": "C:\\test\\GameServer.exe"}' | python lifecycle.py validate
# → {"success": false, "issues": [{"code": "EXE_NOT_FOUND", ...}]}  ← 파일이 없으므로 실패

# ── get_launch_command 테스트 ──
echo '{"server_executable": "C:\\test\\GameServer.exe", "port": 7777}' | python lifecycle.py get_launch_command
# → {"success": true, "program": "...", "args": [...], ...}

# ── configure 테스트 ──
echo '{"working_dir": "C:\\test", "settings": {"server_name": "Hello", "max_players": 32}}' | python lifecycle.py configure
# → {"success": true, "message": "Applied 2 settings"}

# ── import_settings 테스트 ──
echo '{"working_dir": "C:\\test"}' | python lifecycle.py import_settings
# → {"success": true, "settings": {"server_name": "Hello", ...}}

# ── stop 테스트 (실제 PID 사용 시 주의!) ──
echo '{"pid": 99999, "force": true}' | python lifecycle.py stop
```

### 흔한 실수와 해결법

| 증상 | 원인 | 해결 |
|------|------|------|
| `json.decoder.JSONDecodeError` | stdout에 `print()`로 디버그 메시지 출력 | `print(..., file=sys.stderr)` 사용 |
| `ModuleNotFoundError: daemon_rcon` | 데몬 없이 실행할 때 import 실패 | 테스트 시 해당 import를 try/except로 감싸기 |
| 데몬이 모듈을 못 찾음 | 폴더명과 `module.name`이 불일치 | 둘을 동일하게 맞추기 |
| `configure`가 호출됐는데 설정 파일 안 바뀜 | `_KEY_MAP`에 해당 키 매핑 누락 | 매핑 테이블에 키 추가 |
| `get_launch_command` 성공인데 서버 안 뜸 | `program` 경로가 상대경로이거나 잘못됨 | `os.path.abspath()` 사용, 실제 경로 확인 |
| credential_map 동기화 안 됨 | `get_launch_command` 반환값에 게임 키 누락 | 반환 dict에 게임 키=값 포함 확인 |

### pytest로 자동화 테스트

```python
# test_lifecycle.py
import json
import subprocess
import sys

def run_lifecycle(function, config=None):
    """lifecycle.py 함수를 실행하고 결과를 반환"""
    cmd = [sys.executable, "lifecycle.py", function]
    input_data = json.dumps(config or {})
    result = subprocess.run(
        cmd, input=input_data, capture_output=True, text=True
    )
    return json.loads(result.stdout)

def test_validate_no_executable():
    result = run_lifecycle("validate", {
        "server_executable": "/nonexistent/path"
    })
    assert result["success"] is False
    assert any(i["code"] == "EXECUTABLE_NOT_FOUND" for i in result.get("issues", []))

def test_get_launch_command():
    result = run_lifecycle("get_launch_command", {
        "server_executable": "/path/to/server.exe",
        "port": 7777,
    })
    assert "program" in result or result["success"] is False
```

---

## 13. 체크리스트

새 모듈을 작성할 때 확인할 항목입니다.

### 필수

- [ ] `module.toml` 작성
  - [ ] `[module]` 메타데이터 (name, version, entry, icon, log_pattern)
  - [ ] `[protocols]` (supported, default, interaction_mode)
  - [ ] `[config]` (server_executable, process_name, default_port)
  - [ ] `[settings]` — 최소한 port, managed_start, graceful_stop
  - [ ] `[detection]` (process_patterns, dir_signatures)
- [ ] `lifecycle.py` 작성
  - [ ] `validate(config)` — 환경 검증
  - [ ] `get_launch_command(config)` — 실행 커맨드 빌드
  - [ ] `stop(config)` — 종료 처리
  - [ ] `status(config)` — 상태 조회
  - [ ] `configure(config)` — 설정 적용
  - [ ] `import_settings(config)` — 설정 가져오기
  - [ ] `command(config)` — 명령어 실행
  - [ ] CLI 진입점 (`if __name__ == "__main__"`)
- [ ] `locales/en.json` — 영어 번역

### 권장

- [ ] `icon.png` — 모듈 아이콘
- [ ] `[install]` — 설치 방법 정의
- [ ] `[aliases]` — 명령어 별칭
- [ ] `[commands]` — GUI 명령어
- [ ] `[syntax_highlight]` — 콘솔 하이라이팅 (console 모드)
- [ ] `[credential_map]` — 자격증명 동기화 (REST API 사용 시)
- [ ] 추가 locale 파일 (ko, ja, de, es, fr, pt-BR, ru, zh-CN, zh-TW)
- [ ] `diagnose_log()` — 에러 진단
- [ ] `install_server()` — 서버 설치
- [ ] `list_versions()` — 버전 목록 (수동 설치인 경우)
- [ ] 테스트 파일

---

## 14. 실전 예제: 최소 모듈

가장 간단한 형태의 모듈입니다. 수동 설치, RCON 지원 게임을 가정합니다.

### `module.toml`

```toml
[module]
name = "simplegame"
version = "0.1.0"
description = "Simple Game server module"
game_name = "Simple Game"
display_name = "Simple Game"
entry = "lifecycle.py"
icon = "icon.png"
log_pattern = '(?P<level>INFO|WARN|ERROR)'

[protocols]
supported = ["rcon"]
default = "rcon"
interaction_mode = "commands"

[config]
server_executable = "GameServer.exe"
process_name = "GameServer"
default_port = 7777
rcon_port = 25575

[detection]
process_patterns = ["GameServer"]
dir_signatures = ["GameServer.exe"]

[settings]

[[settings.fields]]
name = "port"
type = "number"
label = "Game Port"
default = 7777
min = 1024
max = 65535

[[settings.fields]]
name = "max_players"
type = "number"
label = "Max Players"
default = 16
min = 1
max = 64

[commands]

[[commands.fields]]
name = "save"
label = "Save"
method = "rcon"
rcon_template = "save"
inputs = []

[aliases]
module_aliases = ["sg", "simplegame"]
```

### `lifecycle.py`

```python
#!/usr/bin/env python3
import sys, json, os, time

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
from daemon_rcon import rcon_command as _rcon
from i18n import I18n

i18n = I18n(MODULE_DIR)

def validate(config):
    exe = config.get("server_executable", "")
    issues = []
    if not exe or not os.path.exists(exe):
        issues.append({"code": "EXE_NOT_FOUND", "severity": "critical",
                        "message": "Executable not found", "solution": "Set correct path"})
    return {"success": not any(i["severity"] == "critical" for i in issues), "issues": issues}

def get_launch_command(config):
    exe = config.get("server_executable")
    port = config.get("port", 7777)
    wd = config.get("working_dir") or os.path.dirname(exe)
    return {"success": True, "program": os.path.abspath(exe),
            "args": [f"-port={port}"], "working_dir": os.path.abspath(wd), "env_vars": {}}

def stop(config):
    pid = config.get("pid")
    if not pid:
        return {"success": False, "message": "No PID"}
    rcon_pw = config.get("rcon_password", "")
    if rcon_pw:
        try:
            _rcon("127.0.0.1", config.get("rcon_port", 25575), rcon_pw, "quit")
            for _ in range(15):
                try: os.kill(pid, 0)
                except OSError: return {"success": True, "message": "Stopped"}
                time.sleep(1)
        except Exception: pass
    os.system(f"taskkill /F /PID {pid}" if os.name == "nt" else f"kill -9 {pid}")
    return {"success": True, "message": "Force killed"}

def status(config):
    pid = config.get("pid")
    if pid:
        try:
            os.kill(pid, 0)
            return {"success": True, "status": "running"}
        except OSError: pass
    return {"success": True, "status": "stopped"}

def configure(config):
    return {"success": True, "message": "No config file to update"}

def import_settings(config):
    return {"success": True, "settings": {}}

def command(config):
    return {"success": True, "message": "Command received"}

FUNCTIONS = {"validate": validate, "get_launch_command": get_launch_command,
             "stop": stop, "status": status, "configure": configure,
             "import_settings": import_settings, "command": command}

if __name__ == "__main__":
    fn_name = sys.argv[1] if len(sys.argv) > 1 else ""
    cfg = json.loads(sys.stdin.read() or "{}")
    fn = FUNCTIONS.get(fn_name)
    print(json.dumps(fn(cfg) if fn else {"success": False, "message": f"Unknown: {fn_name}"}))
```

---

## 15. 실전 예제: 전체 기능 모듈

모든 기능을 활용하는 모듈의 module.toml 골격입니다.

```toml
[module]
name = "fullgame"
version = "0.1.0"
description = "Full-featured game server module"
game_name = "Full Game"
display_name = "Full Game"
entry = "lifecycle.py"
icon = "icon.png"
log_pattern = '(?P<level>INFO|WARN|ERROR|DEBUG|FATAL)'

[protocols]
supported = ["rcon", "stdin"]
default = "rcon"
interaction_mode = "console"

[credential_map]
rcon_password = "GameRconPassword"

[config]
executable_path = ""
server_executable = "FullGameServer.exe"
process_name = "FullGameServer"
default_port = 7777
rcon_port = 25575
stop_command = "quit"

[install]
method = "steamcmd"
app_id = 999999
anonymous = true
install_subdir = "server"
platform = "windows"
requires_extensions = ["steamcmd"]

[detection]
process_patterns = ["FullGameServer"]
cmd_patterns = ["fullgame.server"]
common_paths = [
    "C:\\Program Files (x86)\\Steam\\steamapps\\common\\FullGame",
]
dir_signatures = ["FullGameServer.exe", "config.ini"]

[settings]

[[settings.fields]]
name = "managed_start"
type = "boolean"
label = "Managed Start (Console)"
default = true
group = "saba-chan"

[[settings.fields]]
name = "graceful_stop"
type = "boolean"
label = "Graceful Stop"
default = true
group = "saba-chan"

[[settings.fields]]
name = "rcon_port"
type = "number"
label = "RCON Port"
default = 25575
min = 1
max = 65535
group = "saba-chan"

[[settings.fields]]
name = "server_name"
type = "text"
label = "Server Name"
default = "My Full Game Server"

[[settings.fields]]
name = "max_players"
type = "number"
label = "Max Players"
default = 32
min = 1
max = 128

[[settings.fields]]
name = "port"
type = "number"
label = "Game Port"
default = 7777
min = 1024
max = 65535

[[settings.fields]]
name = "server_password"
type = "password"
label = "Server Password"
default = ""

[[settings.fields]]
name = "difficulty"
type = "select"
label = "Difficulty"
options = ["easy", "normal", "hard", "nightmare"]
default = "normal"

[[settings.fields]]
name = "pvp"
type = "boolean"
label = "PvP"
default = true

[[settings.fields]]
name = "tick_rate"
type = "number"
label = "Tick Rate"
default = 64
min = 10
max = 128
group = "advanced"

[commands]

[[commands.fields]]
name = "say"
label = "Send Message"
description = "Broadcast a message to all players"
method = "rcon"
rcon_template = "say {message}"
inputs = [
    { name = "message", type = "text", label = "Message", required = true }
]

[[commands.fields]]
name = "save"
label = "Save World"
method = "rcon"
rcon_template = "save"
inputs = []

[[commands.fields]]
name = "kick"
label = "Kick Player"
method = "rcon"
rcon_template = "kick {player} {reason}"
inputs = [
    { name = "player", type = "text", label = "Player", required = true },
    { name = "reason", type = "text", label = "Reason", required = false }
]

[[commands.fields]]
name = "ban"
label = "Ban Player"
method = "rcon"
rcon_template = "ban {player}"
inputs = [
    { name = "player", type = "text", label = "Player", required = true }
]

[errors]
server_not_running = "Server is not running"
auth_failed = "RCON authentication failed"
connection_refused = "Cannot connect to server"

[aliases]
module_aliases = ["풀게임", "fg", "fullgame"]

[aliases.commands]
start = { aliases = ["시작", "켜기"], description = "Start the server" }
stop = { aliases = ["정지", "끄기"], description = "Stop the server" }
status = { aliases = ["상태"], description = "Check status" }
say = { aliases = ["말하기", "채팅"], description = "Send message" }
save = { aliases = ["저장"], description = "Save world" }
kick = { aliases = ["킥", "강퇴"], description = "Kick player" }
ban = { aliases = ["밴", "차단"], description = "Ban player" }

[syntax_highlight]

[[syntax_highlight.rules]]
name = "timestamp"
pattern = '^\[\d{2}:\d{2}:\d{2}\]'
color = "timestamp"

[[syntax_highlight.rules]]
name = "level_error"
pattern = '(?P<hl>ERROR|FATAL)'
color = "error"
bold = true

[[syntax_highlight.rules]]
name = "level_warn"
pattern = '(?P<hl>WARN(ING)?)'
color = "warn"
bold = true

[[syntax_highlight.rules]]
name = "level_info"
pattern = '(?P<hl>INFO)'
color = "info"

[[syntax_highlight.rules]]
name = "player_join"
pattern = '(?P<hl>\w+) (joined|connected)'
color = "player"
bold = true

[[syntax_highlight.rules]]
name = "player_leave"
pattern = '(?P<hl>\w+) (left|disconnected)'
color = "player"

[[syntax_highlight.rules]]
name = "ip_address"
pattern = '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?'
color = "ip"

[[syntax_highlight.rules]]
name = "server_ready"
pattern = '(?P<hl>Server (started|ready|listening).*)'
color = "success"
bold = true
```

---

## 부록: 게임별 모듈 비교표

| 기능 | Minecraft | Palworld | Zomboid |
|------|-----------|----------|---------|
| **프로토콜** | RCON + stdin | REST | RCON + stdin |
| **interaction_mode** | console | commands | console |
| **설치 방식** | download (Mojang API) | steamcmd | steamcmd |
| **credential_map** | — | `rest_password = "AdminPassword"` | — |
| **설정 파일** | server.properties | PalWorldSettings.ini | servertest.ini |
| **설정 형식** | key=value | UE4 OptionSettings | Key=Value INI |
| **EULA** | ✅ (eula.txt) | — | — |
| **Java 감지** | ✅ (자동 다운로드) | — | ✅ (번들 JRE) |
| **버전 관리** | ✅ (Mojang API) | — | — |
| **로그 진단** | ✅ (ERROR_PATTERNS) | — | — |
| **서버 상태** | SLP 프로토콜 | REST /info + /players | RCON players |
| **syntax_highlight** | 28개 규칙 | 5개 규칙 | 12개 규칙 |

---

*이 문서는 saba-chan-modules 리포지토리의 `__example`, `minecraft`, `palworld`, `zomboid` 모듈을 기반으로 작성되었습니다.*

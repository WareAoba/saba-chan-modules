"""module.toml의 명령 답 모양 (데몬 채팅 답이 쓰는 정규식)

- failure_patterns: 서버가 명령을 거절한 줄만 실패로 본다. 공지·채팅 본문에 같은 문구가 있어도 실패가 아니다
- seed reply_pattern: 콘솔 답에서 시드 숫자를 뽑는다
"""
import pathlib
import re
import tomllib

MANIFEST = tomllib.loads((pathlib.Path(__file__).parent / "module.toml").read_text(encoding="utf-8-sig"))
FAILURES = [re.compile(p) for p in MANIFEST["commands"]["failure_patterns"]]


def failure(console):
    for pattern in FAILURES:
        m = pattern.search(console)
        if m:
            return m.group("reason")
    return None


def test_rejections_are_failures():
    for line, reason in [
        ("[12:00:00] [Server thread/INFO]: No player was found", "No player was found"),
        # 26.x는 명령 답 앞에 `System chat: `을 붙인다 (PC 정규 설치 26.3 `list` 답에서 확인)
        ("[12:00:00] [Server thread/INFO]: System chat: No player was found", "No player was found"),
        ("[12:00:00] [Server thread/INFO]: Unknown or incomplete command. See below for error", "Unknown or incomplete command"),
        ("[12:00:00 INFO]: Incorrect argument for command", "Incorrect argument for command"),
        ("[12:00:00] [Server thread/INFO]: Unknown item 'minecraft:dimond'", "Unknown item 'minecraft:dimond'"),
    ]:
        assert failure(f"[11:59:59] [Server thread/INFO]: other\n{line}") == reason, line


def test_the_same_words_in_a_message_are_not_failures():
    for line in [
        "[12:00:00] [Server thread/INFO]: [Server] No player was found",
        "[12:00:00] [Server thread/INFO]: <Steve> Unknown or incomplete command",
        "[12:00:00] [Server thread/INFO]: Gave 1 [Diamond] to Steve",
    ]:
        assert failure(line) is None, line


def test_seed_reply_reads_the_number():
    field = next(f for f in MANIFEST["commands"]["fields"] if f["name"] == "seed")
    m = re.search(field["reply_pattern"], "[12:00:00] [Server thread/INFO]: Seed: [-4172144997902289642]")
    assert m and m.group("seed") == "-4172144997902289642"

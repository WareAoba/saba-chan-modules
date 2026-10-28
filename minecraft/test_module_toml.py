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


GRACEFUL = [re.compile(p) for p in MANIFEST["detection"]["graceful_exit_patterns"]]


def graceful(line):
    return any(p.search(line) for p in GRACEFUL)


def test_stop_command_answer_is_a_graceful_exit():
    for line in [
        "[12:00:00] [Server thread/INFO]: Stopping the server",
        # 26.x는 명령 답 앞에 `System chat: `을 붙인다 (PC 정규 설치 26.3 파이프 실측의 stop 답)
        "[12:00:00] [Server thread/INFO]: System chat: Stopping the server",
        "[12:00:00 INFO]: Stopping the server",
    ]:
        assert graceful(line), line


def test_chat_with_the_stop_words_is_not_a_graceful_exit():
    for line in [
        "[12:00:00] [Server thread/INFO]: <Steve> Stopping the server",
        "[12:00:00] [Server thread/INFO]: [Server] Stopping the server",
        "[12:00:00] [Server thread/INFO]: System chat: <Steve> Stopping the server",
    ]:
        assert not graceful(line), line


# ─── 실제 서버 줄 (클라우드 Linux 실측 2026-10-28: 26.3 바닐라·Paper·Forge·NeoForge·Fabric·Spigot, 26.1 바닐라·NeoForge, Folia 26.1.2) ───
# 로그 머리 세 모양: 바닐라·Fabric·Quilt·Spigot `[시각] [스레드/레벨]: `, Paper 계열 `[시각 레벨]: `,
# Forge·NeoForge `[시각] [스레드/레벨] [minecraft/로거]: `. 26.3은 명령 답 앞에 `System chat: `을 붙이고 26.1은 붙이지 않는다
HEADS = ["[20:36:46] [Server thread/INFO]: ", "[19:56:12 INFO]: ", "[20:38:12] [Server thread/INFO] [minecraft/MinecraftServer]: "]


def test_a_missing_teleport_target_is_a_failure():
    """`tp <없는 이름> …`의 답은 `No entity was found`다 (`No player was found`가 아니다)"""
    for head in HEADS:
        for body in ["No entity was found", "System chat: No entity was found"]:
            assert failure(head + body) == "No entity was found", head + body


EXT = MANIFEST["extensions"]["saba-core"]
JOIN = [re.compile(p) for p in EXT["player_join_patterns"]]
LEAVE = [re.compile(p) for p in EXT["player_leave_patterns"]]


def test_join_and_leave_lines_of_every_server_family():
    for head in HEADS:
        assert any(p.search(head + "SabaBot joined the game") for p in JOIN), head
        assert any(p.search(head + "SabaBot left the game") for p in LEAVE), head


RULES = {r["name"]: r for r in MANIFEST["syntax_highlight"]["rules"]}


def highlight(name, line):
    m = re.search(RULES[name]["pattern"], line)
    if not m:
        return None
    return m.group("hl") if "hl" in m.re.groupindex else m.group(0)


def test_rejected_commands_are_colored_as_errors():
    for head in HEADS:
        for body, hl in [
            ("System chat: No player was found", "No player was found"),
            ("No entity was found", "No entity was found"),
            ("System chat: Unknown or incomplete command. See below for error", "Unknown or incomplete command. See below for error"),
            ("System chat: Incorrect argument for command", "Incorrect argument for command"),
            ("System chat: Unknown item 'minecraft:not_an_item_saba'", "Unknown item 'minecraft:not_an_item_saba'"),
        ]:
            assert highlight("command_rejected", head + body) == hl, head + body
    assert highlight("command_rejected", "[20:00:00] [Server thread/INFO]: [Not Secure] [Server] No player was found") is None
    assert highlight("command_rejected", "[20:00:00] [Server thread/INFO]: <Steve> No entity was found") is None
    # 틀린 자리 표시: 오류 다음 줄 `System chat: ...ive Steve not_an_item_saba<--[HERE]`
    assert highlight("command_error_here", "[20:00:00 INFO]: System chat: ...ive Steve not_an_item_saba<--[HERE]") == "<--[HERE]"


def test_admin_feedback_of_26x_is_highlighted():
    for body, hl in [
        ("System chat: Made Steve a server operator", "Made Steve a server operator"),
        ("System chat: Made Steve no longer a server operator", "Made Steve no longer a server operator"),
        ("System chat: Added Steve to the whitelist", "Added Steve to the whitelist"),
        ("System chat: Removed Steve from the whitelist", "Removed Steve from the whitelist"),
        ("System chat: Banned Steve: saba test", "Banned Steve"),
        ("System chat: Unbanned Steve", "Unbanned Steve"),
        ("Kicked SabaBot: saba test", "Kicked SabaBot"),
        ("Teleported SabaBot to 0.500000, 100.000000, 0.500000", "Teleported SabaBot"),
        ("Set SabaBot's game mode to Creative Mode", "Set SabaBot's game mode"),
    ]:
        for head in HEADS:
            assert highlight("admin_target", head + body) == hl, head + body


def test_bukkit_unknown_command_is_a_failure():
    """Spigot은 모르는 명령에 `Unknown command. Type "/help" for help.`(spigot.yml messages.unknown-command),
    CraftBukkit은 `Type "help"`로 답한다 (실측 26.1·26.3)"""
    for line in ['[21:20:21] [Server thread/INFO]: Unknown command. Type "/help" for help.',
                 '[22:00:00] [Server thread/INFO]: Unknown command. Type "help" for help.']:
        assert failure(line) == "Unknown command", line
        assert highlight("command_rejected", line) == line.split("]: ", 1)[1]
    assert failure('[22:00:00] [Server thread/INFO]: <Steve> Unknown command. Type "help" for help.') is None


def test_spigot_kick_leave_line_ends_with_a_period():
    """Spigot·CraftBukkit은 추방·밴으로 나갈 때 `left the game.`을 찍는다 (실측 26.1, 보통 퇴장은 마침표가 없다)"""
    line = "[20:49:16] [Server thread/INFO]: SabaBot left the game."
    m = next((p.search(line) for p in LEAVE if p.search(line)), None)
    assert m and m.group("name") == "SabaBot"
    assert highlight("player_leave", line) == "SabaBot"


def test_player_events_are_bound_to_the_message_start():
    head = HEADS[0]
    assert highlight("player_join", head + "SabaBot joined the game") == "SabaBot"
    assert highlight("player_leave", head + "SabaBot left the game") == "SabaBot"
    assert highlight("player_join", head + "<Steve> Bob joined the game") is None
    assert highlight("player_leave", head + "[Not Secure] [Server] Bob left the game") is None
    assert highlight("player_login", head + "SabaBot[/127.0.0.1:57348] logged in with entity id 21 at (0.5, 68.0, 0.5)") == "SabaBot"
    assert highlight("player_disconnect_reason", head + "SabaBot lost connection: You are banned from this server") == "You are banned from this server"


def test_structure_of_the_three_log_heads_is_dimmed():
    assert highlight("system_chat", HEADS[0] + "System chat: Saved the game") == "System chat:"
    assert highlight("not_secure", HEADS[1] + "[Not Secure] [Server] 사바쨩 시험 공지 test") == "[Not Secure]"
    assert highlight("forge_logger", HEADS[2] + "Done (5.2s)!") == "[minecraft/MinecraftServer]"
    assert highlight("plugin_tag", "[19:55:12 INFO]: [spark] Starting background profiler...") == "[spark]"
    assert highlight("plugin_tag", "[19:55:12 INFO]: [Not Secure] [Server] hi") is None
    # JVM이 표준 오류로 내는 경고 (로그 머리 없음)
    assert highlight("jvm_warning", "WARNING: A restricted method in java.lang.System has been called")

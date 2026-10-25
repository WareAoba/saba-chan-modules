"""픽스처 게임 서버: 콘솔 명령을 받고, 저장할 때 world/와 notes.txt를 쓴다 (notes.txt는 일부러 백업 대상 밖)"""
import json
import os
import sys


def save():
    os.makedirs("world", exist_ok=True)
    with open(os.path.join("world", "level.dat"), "a", encoding="utf-8") as f:
        f.write("saved\n")
    with open("notes.txt", "a", encoding="utf-8") as f:
        f.write("note\n")


settings = json.load(open("server.json", encoding="utf-8")) if os.path.exists("server.json") else {}
print(f"Starting trialgame on port {settings.get('port')}", flush=True)
print("Done, type help", flush=True)
for line in sys.stdin:
    cmd = line.strip()
    if cmd == "help":
        # 일부러 kick을 빠뜨린다
        print("Commands: say <message>, save, stop", flush=True)
    elif cmd.startswith("say "):
        print(f"[Server] {cmd[4:]}", flush=True)
    elif cmd == "save":
        save()
        print("Saved", flush=True)
    elif cmd == "stop":
        save()
        print("Saved and stopped", flush=True)
        break

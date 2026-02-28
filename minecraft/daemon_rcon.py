"""
saba-chan Daemon RCON Bridge
==============================
Lightweight helper for Python lifecycle modules to execute RCON commands
via the Rust daemon's HTTP API rather than opening raw TCP sockets.

The Rust daemon at ``/api/instance/:id/rcon`` already has a robust RCON
client with retry logic, connection pooling, and proper error handling.
This module delegates to it.

Usage (one-shot, replaces extensions.rcon.rcon_command):
    from daemon_rcon import daemon_rcon_command
    response = daemon_rcon_command(instance_id, "list")

Usage (session-like, replaces extensions.rcon.RconClient):
    from daemon_rcon import DaemonRconClient
    client = DaemonRconClient(instance_id)
    response = client.command("save")
    response2 = client.command("quit")

Environment:
    DAEMON_API_URL  — base URL of the daemon (default: http://127.0.0.1:57474)
"""

import json
import os
import sys
import urllib.request
import urllib.error

# ─── Constants ────────────────────────────────────────────────

_DEFAULT_DAEMON_URL = "http://127.0.0.1:57474"


def _daemon_url():
    """Resolve the daemon API base URL."""
    return os.environ.get("DAEMON_API_URL", _DEFAULT_DAEMON_URL)


# ─── One-shot function ────────────────────────────────────────

def daemon_rcon_command(instance_id, command, *, rcon_port=None, rcon_password=None, timeout=10):
    """Execute a single RCON command via the daemon's HTTP API.

    This replaces direct TCP RCON communication. The daemon handles
    connection, authentication, retry, and error handling.

    Args:
        instance_id: The server instance ID.
        command: RCON command string to execute.
        rcon_port: Optional override for RCON port (daemon uses instance config if None).
        rcon_password: Optional override for RCON password.
        timeout: HTTP request timeout in seconds.

    Returns:
        str: Response text from the server.

    Raises:
        ConnectionError: If the daemon is unreachable or RCON fails.
    """
    url = f"{_daemon_url()}/api/instance/{instance_id}/rcon"

    payload = {"command": command}
    if rcon_port is not None:
        payload["rcon_port"] = int(rcon_port)
    if rcon_password is not None:
        payload["rcon_password"] = rcon_password

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        if result.get("error"):
            raise ConnectionError(f"RCON failed: {result['error']}")

        # Extract response text from data.data or data itself
        response_data = result.get("data")
        if isinstance(response_data, dict):
            return response_data.get("data", str(response_data))
        if response_data is not None:
            return str(response_data)
        return result.get("message", "")

    except urllib.error.URLError as e:
        raise ConnectionError(f"Daemon unreachable at {url}: {e}") from e
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise ConnectionError(f"RCON request failed (HTTP {e.code}): {body}") from e


# ─── Session-like class ──────────────────────────────────────

class DaemonRconClient:
    """RCON client that delegates all commands to the daemon's HTTP API.

    Drop-in replacement for extensions.rcon.RconClient where lifecycle
    modules used session-based RCON. Since each HTTP call is independent,
    connect/disconnect are no-ops, but the API is preserved for compatibility.
    """

    def __init__(self, instance_id, host="127.0.0.1", port=25575, password=""):
        """Initialize the daemon-delegating RCON client.

        Args:
            instance_id: The server instance ID (required for daemon API).
            host: Ignored (daemon connects to the right host).
            port: RCON port (passed to daemon as override).
            password: RCON password (passed to daemon as override).
        """
        self.instance_id = instance_id
        self.host = host
        self.port = int(port)
        self.password = password
        self._connected = False

    def connect(self):
        """No-op — the daemon manages connections.

        Returns:
            bool: Always True (actual connection happens per-command).
        """
        self._connected = True
        return True

    def disconnect(self):
        """No-op — the daemon manages connections."""
        self._connected = False

    def command(self, cmd):
        """Send a command via the daemon RCON API.

        Args:
            cmd: Command string to execute.

        Returns:
            str or None: Response text, or None on failure.
        """
        try:
            return daemon_rcon_command(
                self.instance_id,
                cmd,
                rcon_port=self.port,
                rcon_password=self.password,
            )
        except ConnectionError as e:
            print(f"[RCON] Daemon bridge failed: {e}", file=sys.stderr)
            return None

    @property
    def socket(self):
        """Compatibility shim — returns a truthy value when 'connected'."""
        return self._connected or None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()


# ─── Legacy-compatible one-shot (signature matches extensions.rcon.rcon_command) ──

def rcon_command(host, port, password, command, timeout=5, *, instance_id=None):
    """Legacy-compatible one-shot RCON via daemon.

    If instance_id is not provided, falls back to a direct RCON connection
    as a last resort (for backward compatibility during transition).
    When instance_id is available, delegates to the daemon.

    Args:
        host: RCON server host (ignored when using daemon).
        port: RCON server port.
        password: RCON password.
        command: Command string to execute.
        timeout: Timeout in seconds.
        instance_id: Server instance ID for daemon delegation.

    Returns:
        str: Response text.

    Raises:
        ConnectionError: If communication fails.
    """
    if instance_id:
        return daemon_rcon_command(
            instance_id, command,
            rcon_port=port, rcon_password=password, timeout=timeout,
        )

    # Fallback: direct socket RCON (same implementation as old rcon.py)
    # This path is only used when instance_id is not available
    import socket as _socket
    import struct as _struct
    import random as _random

    SERVERDATA_AUTH = 3
    SERVERDATA_EXECCOMMAND = 2

    def _make_packet(req_id, pkt_type, payload):
        body = _struct.pack("<ii", req_id, pkt_type) + payload + b"\x00\x00"
        return _struct.pack("<i", len(body)) + body

    def _read_packet(sock):
        raw_size = b""
        while len(raw_size) < 4:
            chunk = sock.recv(4 - len(raw_size))
            if not chunk:
                return None
            raw_size += chunk
        size = _struct.unpack("<i", raw_size)[0]
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                return None
            data += chunk
        req_id = _struct.unpack("<i", data[:4])[0]
        pkt_type = _struct.unpack("<i", data[4:8])[0]
        payload = data[8:-2] if len(data) > 10 else b""
        return (req_id, pkt_type, payload)

    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, int(port)))

        # Auth
        req_id = _random.randint(1, 2147483647)
        sock.sendall(_make_packet(req_id, SERVERDATA_AUTH, password.encode("utf-8")))
        pkt = _read_packet(sock)
        if pkt is None or pkt[0] == -1:
            raise ConnectionError("RCON authentication failed")

        # Command
        req_id = _random.randint(1, 2147483647)
        sock.sendall(_make_packet(req_id, SERVERDATA_EXECCOMMAND, command.encode("utf-8")))
        pkt = _read_packet(sock)
        if pkt is None:
            raise ConnectionError("RCON command returned no response")
        return pkt[2].decode("utf-8", errors="replace")
    finally:
        sock.close()

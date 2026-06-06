# pyright: strict
"""``--check --bind``: prove the real UDP bind path on loopback.

Every other check is offline (``check_listen_host`` asserts value plumbing only).
This drives the production ``Server.bind()`` seam (→ ``Server._bind``) against a
loopback ``Config`` with an OS-ephemeral port, then asserts the bound socket's
configured host (127.0.0.1, not 0.0.0.0), ``AF_INET`` / ``SOCK_DGRAM`` family/
type, production recv timeout, and a non-zero assigned port, then closes it —
exercising the exact listen-socket setup the collector uses in production, with
no fixed port to collide with a running collector. Task 8 adds the bind-FAILURE
oracle (``_check_bind_failure``) to the same module.
"""

from __future__ import annotations

import socket
import sys

from .. import config
from ..server import RECV_TIMEOUT_S, Server
from .fakes import CaptureWriter
from .options import default_check_options


def _loopback_bind_config() -> config.Config:
    """A valid loopback Config with an OS-ephemeral port.

    config.validate rejects listen_port=0 (production floor _MIN_PORT=1), so validate the rest of
    the fields then _replace the port with the 0 ephemeral sentinel — exercising every other
    validated field while letting the OS pick a free port for a collision-free smoke.
    """
    validated = config.validate({**default_check_options(), "listen_host": "127.0.0.1"})
    return validated._replace(listen_port=0)


def check_bind() -> bool:
    """Bind the production listen socket on loopback and assert its shape."""
    ok = True
    cfg = _loopback_bind_config()
    server = Server(cfg, CaptureWriter())
    sock = server.bind()  # public production bind seam under test
    try:
        host = sock.getsockname()[0]
        port = sock.getsockname()[1]
        bound = port != 0
        host_ok = host == "127.0.0.1"
        family_ok = sock.family == socket.AF_INET
        type_ok = sock.type == socket.SOCK_DGRAM
        timeout_ok = sock.gettimeout() == RECV_TIMEOUT_S
        checks = [
            (
                f"production Server._bind bound an ephemeral port (127.0.0.1:{port})",
                bound,
            ),
            (
                f"bound to configured listen_host 127.0.0.1 (not 0.0.0.0); got {host}",
                host_ok,
            ),
            ("bound socket family is AF_INET", family_ok),
            ("bound socket type is SOCK_DGRAM", type_ok),
            (
                f"production recv timeout applied (RECV_TIMEOUT_S={RECV_TIMEOUT_S})",
                timeout_ok,
            ),
        ]
        for label, passed in checks:
            print(f"{'PASS' if passed else 'FAIL'}  bind: {label}", file=sys.stderr)
            ok = ok and passed
    finally:
        sock.close()
    closed_clean = sock.fileno() == -1
    print(
        f"{'PASS' if closed_clean else 'FAIL'}  bind: socket closed cleanly",
        file=sys.stderr,
    )
    ok = ok and closed_clean
    print(f"BIND CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok

from __future__ import annotations

import socket

from . import create_app


def _pick_port(preferred: int) -> int:
    # Try preferred first, otherwise pick any free port.
    candidates = [preferred] + list(range(max(preferred + 1, 1024), max(preferred + 1, 1024) + 50))
    for port in candidates:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    # Last resort: let OS choose
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def main() -> None:
    app = create_app()
    settings = app.config["AUDIT_SETTINGS"]
    # Internal-only: bind to localhost by default
    port = _pick_port(int(getattr(settings, "port", 5055) or 5055))
    app.run(host="127.0.0.1", port=port, debug=True)


if __name__ == "__main__":
    main()


from __future__ import annotations

import os
import time
import urllib.parse

import requests


def main() -> None:
    # This script assumes Next.js is running on localhost:3000 and proxies /audit/* to the portal.
    base = os.getenv("PROBE_NEXT_BASE", "http://localhost:3000").rstrip("/")
    target = os.getenv("PROBE_TARGET_BASE_URL", "http://localhost:3000").rstrip("/")

    print("next_base:", base)
    print("set_target:", target)

    s = requests.Session()

    # Save target
    r = s.post(
        f"{base}/audit/target",
        data={"base_url": target},
        timeout=20,
        allow_redirects=False,
    )
    print("POST /audit/target:", r.status_code, "location:", r.headers.get("location"))

    # Trigger recrawl
    r2 = s.post(
        f"{base}/audit/recrawl",
        data={},
        timeout=20,
        allow_redirects=False,
    )
    print("POST /audit/recrawl:", r2.status_code, "location:", r2.headers.get("location"))

    # Poll dashboard a few times (optional)
    for i in range(5):
        time.sleep(2)
        rd = s.get(f"{base}/audit/", timeout=20)
        print("GET /audit/:", rd.status_code, "len:", len(rd.text))


if __name__ == "__main__":
    main()


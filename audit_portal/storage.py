from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


_lock = threading.Lock()


def _data_dir() -> str:
    # Store under audit-portal/data (next to package), not inside instance/sqlite.
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(here), "data")


def _state_path() -> str:
    return os.path.join(_data_dir(), "latest.json")


def ensure_storage() -> None:
    os.makedirs(_data_dir(), exist_ok=True)
    if not os.path.exists(_state_path()):
        save_state(new_state())


def new_state() -> Dict[str, Any]:
    return {
        "target_base_url": "",
        "run": {
            "id": 0,
            "status": "idle",  # idle|running|done|error|cancelled
            "started_at": None,
            "finished_at": None,
            "error_message": None,
            "pages_discovered": 0,
            "pages_fetched": 0,
        },
        "pages": [],  # list[dict]
        "links": [],  # list[dict] (body internal links + anchors)
    }


def load_state() -> Dict[str, Any]:
    ensure_storage()
    with _lock:
        with open(_state_path(), "r", encoding="utf-8") as f:
            return json.load(f)


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(_data_dir(), exist_ok=True)
    tmp = _state_path() + ".tmp"
    with _lock:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, _state_path())


def wipe_results_keep_target() -> Dict[str, Any]:
    state = load_state()
    target = (state.get("target_base_url") or "").strip()
    run_id = int(state.get("run", {}).get("id") or 0) + 1
    fresh = new_state()
    fresh["target_base_url"] = target
    fresh["run"]["id"] = run_id
    save_state(fresh)
    return fresh


def set_target_base_url(url: str) -> None:
    url = (url or "").strip().rstrip("/")
    state = load_state()
    state["target_base_url"] = url
    save_state(state)


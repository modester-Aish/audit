from __future__ import annotations

import json
import os
import threading
from typing import Any, Dict, List, Optional


_lock = threading.Lock()


def _data_dir() -> str:
    # Vercel (and similar) mount the deployment as read-only; only /tmp is writable.
    if os.environ.get("VERCEL"):
        base = os.environ.get("TMPDIR") or "/tmp"
        return os.path.join(base, "audit-portal-data")
    # Store under project/data (next to package), not inside instance/sqlite.
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


def _runs_dir() -> str:
    return os.path.join(_data_dir(), "runs")


def _run_index_path() -> str:
    return os.path.join(_runs_dir(), "index.json")


def _run_snapshot_path(run_id: int) -> str:
    return os.path.join(_runs_dir(), f"{run_id}.json")


def archive_completed_run(state: Dict[str, Any]) -> None:
    run = state.get("run") or {}
    rid = int(run.get("id") or 0)
    if rid <= 0:
        return
    status = str(run.get("status") or "")
    if status not in ("done", "error"):
        return
    os.makedirs(_runs_dir(), exist_ok=True)
    path = _run_snapshot_path(rid)
    tmp = path + ".tmp"
    with _lock:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, path)
        _merge_run_index_summary(_summary_from_state(state))


def _summary_from_state(state: Dict[str, Any]) -> Dict[str, Any]:
    run = state.get("run") or {}
    return {
        "id": int(run.get("id") or 0),
        "status": run.get("status"),
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
        "target_base_url": (state.get("target_base_url") or ""),
        "pages_count": len(state.get("pages") or []),
        "links_count": len(state.get("links") or []),
        "error_message": run.get("error_message"),
    }


def _merge_run_index_summary(summary: Dict[str, Any]) -> None:
    path = _run_index_path()
    rows: List[Dict[str, Any]] = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            rows = json.load(f)
    rid = int(summary.get("id") or 0)
    rows = [r for r in rows if int(r.get("id") or 0) != rid]
    rows.insert(0, summary)
    rows = rows[:100]
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    os.replace(tmp, path)


def list_run_history() -> List[Dict[str, Any]]:
    ensure_storage()
    path = _run_index_path()
    if not os.path.exists(path):
        return []
    with _lock:
        with open(path, "r", encoding="utf-8") as f:
            return list(json.load(f))


def load_archived_run(run_id: int) -> Optional[Dict[str, Any]]:
    if run_id <= 0:
        return None
    ensure_storage()
    path = _run_snapshot_path(run_id)
    if not os.path.exists(path):
        return None
    with _lock:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


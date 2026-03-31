from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

from flask import current_app

from .crawler import (
    SimpleCrawler,
    extract_body_internal_links,
    extract_page_meta,
    format_index_explanation,
    is_indexable,
    normalize_url,
)
from .storage import archive_completed_run, load_state, save_state, set_target_base_url as store_set_target, wipe_results_keep_target

_crawl_lock = threading.Lock()
_scheduler_started = False
_app_for_threads = None


def get_target_base_url() -> str:
    state = load_state()
    target = (state.get("target_base_url") or "").strip()
    if target:
        return target
    settings = current_app.config["AUDIT_SETTINGS"]
    return (settings.base_url or "").strip().rstrip("/")


def set_target_base_url(url: str) -> None:
    store_set_target(url)


def ensure_auto_recrawl_scheduler() -> None:
    global _scheduler_started
    if _scheduler_started:
        return

    # Capture the Flask app object for background threads.
    global _app_for_threads
    _app_for_threads = current_app._get_current_object()

    settings = current_app.config["AUDIT_SETTINGS"]
    if getattr(settings, "auto_recrawl_minutes", 0) <= 0:
        return

    _scheduler_started = True
    t = threading.Thread(target=_auto_recrawl_loop, daemon=True)
    t.start()


def _auto_recrawl_loop() -> None:
    app = _app_for_threads
    while True:
        with app.app_context():
            settings = current_app.config["AUDIT_SETTINGS"]
            minutes = max(int(getattr(settings, "auto_recrawl_minutes", 0)), 0)
            if minutes <= 0:
                # Config changed; stop loop.
                return

            state = load_state()
            if (state.get("run") or {}).get("status") == "running":
                pass
            else:
                # Only auto-run if BASE_URL is set.
                if get_target_base_url():
                    start_crawl_async()

        time.sleep(minutes * 60)


def start_crawl_async() -> int:
    with _crawl_lock:
        # If a crawl is already running, mark it cancelled.
        prev = load_state()
        if (prev.get("run") or {}).get("status") == "running":
            prev["run"]["status"] = "cancelled"
            prev["run"]["finished_at"] = datetime.utcnow().isoformat()
            save_state(prev)

        state = wipe_results_keep_target()
        run_id = int(state["run"]["id"])
        state["run"]["status"] = "running"
        state["run"]["started_at"] = datetime.utcnow().isoformat()
        state["run"]["finished_at"] = None
        state["run"]["error_message"] = None
        state["run"]["pages_discovered"] = 0
        state["run"]["pages_fetched"] = 0
        save_state(state)

    # Capture app object now (request/app context exists here), then pass to thread.
    app = current_app._get_current_object()
    t = threading.Thread(target=_crawl_in_app_context, args=(app, run_id), daemon=True)
    t.start()
    return run_id


def _crawl_in_app_context(app, run_id: int) -> None:
    with app.app_context():
        _crawl_run(run_id)


def _crawl_run(run_id: int) -> None:
    settings = current_app.config["AUDIT_SETTINGS"]
    base_url = get_target_base_url()
    try:
        crawler = SimpleCrawler(
            base_url,
            max_pages=settings.max_pages,
            request_timeout_seconds=settings.request_timeout_seconds,
            user_agent=settings.user_agent,
            crawl_delay_seconds=settings.crawl_delay_seconds,
            use_sitemap_seed=settings.use_sitemap_seed,
            sitemap_seed_cap=min(settings.max_pages, settings.sitemap_seed_cap),
        )

        pages_discovered = 0
        pages_fetched = 0

        state = load_state()
        state["pages"] = []
        state["links"] = []

        for res in crawler.crawl():
            # If user started a newer crawl, stop this one immediately.
            current = load_state()
            if int((current.get("run") or {}).get("id") or 0) != int(run_id):
                return
            if (current.get("run") or {}).get("status") == "cancelled":
                return

            pages_discovered += 1
            pages_fetched += 1 if res.status_code is not None else 0

            meta_robots = res.meta_robots or ""
            x_robots = res.x_robots_tag or ""
            idx, reason = is_indexable(res.status_code, meta_robots, x_robots)

            page_url = normalize_url(res.url)
            page: Dict[str, Any] = {
                "url": page_url,
                "canonical_url": normalize_url(res.canonical_url) if res.canonical_url else None,
                "status_code": res.status_code,
                "content_type": res.content_type,
                "meta_robots": meta_robots,
                "x_robots_tag": x_robots,
                "indexable": bool(idx),
                "index_reason": reason,
                "index_explanation": format_index_explanation(
                    reason,
                    status_code=res.status_code,
                    meta_robots=meta_robots,
                    x_robots_tag=x_robots,
                ),
                "body_internal_link_count": 0,
                "has_body_internal_links": False,
                "title": None,
                "meta_description": None,
                "og_title": None,
                "og_description": None,
                "twitter_title": None,
                "twitter_description": None,
                "h1": None,
                "display_title": None,
                "display_description": None,
                "title_source": None,
                "description_source": None,
                "response_time_ms": res.response_time_ms,
            }

            # Body-only internal links + anchors
            if res.html:
                page.update(extract_page_meta(res.html))
                links = extract_body_internal_links(res.html, res.url, base_url)
                page["body_internal_link_count"] = len(links)
                page["has_body_internal_links"] = len(links) > 0

                for to_url, anchor in links:
                    state["links"].append(
                        {
                            "from_page_url": page_url,
                            "to_url": to_url,
                            "anchor_text": anchor,
                            "is_internal": True,
                            "in_body": True,
                        }
                    )

            state["pages"].append(page)

            # Save progress frequently so you can see data appearing early.
            if pages_discovered == 1 or pages_discovered % 5 == 0:
                state["run"]["pages_discovered"] = pages_discovered
                state["run"]["pages_fetched"] = pages_fetched
                save_state(state)

        state["run"]["status"] = "done"
        state["run"]["finished_at"] = datetime.utcnow().isoformat()
        state["run"]["pages_discovered"] = pages_discovered
        state["run"]["pages_fetched"] = pages_fetched
        save_state(state)
        archive_completed_run(load_state())

    except Exception as e:
        state = load_state()
        state["run"]["status"] = "error"
        state["run"]["finished_at"] = datetime.utcnow().isoformat()
        state["run"]["error_message"] = str(e)
        save_state(state)
        archive_completed_run(load_state())


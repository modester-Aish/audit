from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from flask import Blueprint, current_app, redirect, render_template, request, url_for

from .auth import require_basic_auth
from .service import get_target_base_url, set_target_base_url, start_crawl_async
from .storage import load_state

bp = Blueprint("routes", __name__)


@bp.before_request
def _auth_guard():
    # Secure & internal: require basic auth for all endpoints.
    settings = current_app.config["AUDIT_SETTINGS"]
    if settings.require_auth:
        resp = require_basic_auth()
        if resp:
            return resp
    return None


@bp.after_request
def _noindex_headers(resp):
    # Prevent indexing even if exposed behind a proxy.
    resp.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    resp.headers["Cache-Control"] = "no-store"
    return resp


@bp.get("/robots.txt")
def robots_txt():
    return ("User-agent: *\nDisallow: /\n", 200, {"Content-Type": "text/plain; charset=utf-8"})

@dataclass(frozen=True)
class DashboardFilters:
    q: str
    index: str
    links: str
    anchor: str


def _filtered_pages(state: Dict[str, object], filters: DashboardFilters) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    all_pages: List[Dict[str, object]] = list(state.get("pages") or [])
    all_links: List[Dict[str, object]] = list(state.get("links") or [])

    def _page_has_anchor_match(page_url: str) -> bool:
        needle = filters.anchor.lower()
        for l in all_links:
            if l.get("from_page_url") != page_url:
                continue
            a = (l.get("anchor_text") or "")
            if needle in a.lower():
                return True
        return False

    filtered = all_pages
    if filters.q:
        qneedle = filters.q.lower()
        filtered = [p for p in filtered if qneedle in str(p.get("url", "")).lower()]
    if filters.index == "indexable":
        filtered = [p for p in filtered if p.get("indexable") is True]
    elif filters.index == "non_indexable":
        filtered = [p for p in filtered if p.get("indexable") is False]
    if filters.links == "has":
        filtered = [p for p in filtered if p.get("has_body_internal_links") is True]
    elif filters.links == "none":
        filtered = [p for p in filtered if p.get("has_body_internal_links") is False]
    if filters.anchor:
        filtered = [p for p in filtered if _page_has_anchor_match(str(p.get("url", "")))]

    filtered.sort(key=lambda p: str(p.get("url", "")))
    return filtered, all_links


@bp.get("/")
def dashboard():
    settings = current_app.config["AUDIT_SETTINGS"]
    target_base_url = get_target_base_url()
    state = load_state()
    run = state.get("run") or {}

    filters = DashboardFilters(
        q=(request.args.get("q") or "").strip(),
        index=(request.args.get("index") or "").strip(),
        links=(request.args.get("links") or "").strip(),
        anchor=(request.args.get("anchor") or "").strip(),
    )

    all_pages: List[Dict[str, object]] = list(state.get("pages") or [])
    all_links: List[Dict[str, object]] = list(state.get("links") or [])

    pages: List[Dict[str, object]] = []
    stats = {
        "total_pages": len(all_pages),
        "indexable": sum(1 for p in all_pages if p.get("indexable") is True),
        "non_indexable": sum(1 for p in all_pages if p.get("indexable") is False),
        "with_internal_links": sum(1 for p in all_pages if p.get("has_body_internal_links") is True),
    }

    filtered, all_links_for_filter = _filtered_pages(state, filters)
    pages = filtered[:500]

    # Anchor samples for each page
    for p in pages:
        url = str(p.get("url", ""))
        samples: List[str] = []
        for l in all_links_for_filter:
            if l.get("from_page_url") != url:
                continue
            a = (l.get("anchor_text") or "").strip()
            if a:
                samples.append(a)
            if len(samples) >= 12:
                break
        p["anchor_sample"] = samples
        p["audit_links_url"] = "/audit/links?url=" + url

    return render_template(
        "dashboard.html",
        settings=settings,
        target_base_url=target_base_url,
        latest_run=run,
        filters=filters,
        pages=pages,
        stats=stats,
        filtered_count=len(filtered),
    )


@bp.post("/target")
def set_target():
    url = (request.form.get("base_url") or "").strip()
    set_target_base_url(url)
    return redirect("/audit/")


@bp.post("/recrawl")
def recrawl():
    # Start a new run (async) and redirect to dashboard.
    start_crawl_async()
    return redirect("/audit/")


@bp.get("/export/pages.csv")
def export_pages_csv():
    import csv
    import io

    state = load_state()
    rows = list(state.get("pages") or [])

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "url",
            "canonical_url",
            "status_code",
            "content_type",
            "meta_robots",
            "x_robots_tag",
            "indexable",
            "index_reason",
            "body_internal_link_count",
            "has_body_internal_links",
        ]
    )
    for r in rows:
        w.writerow(
            [
                r.get("url", ""),
                r.get("canonical_url") or "",
                r.get("status_code") or "",
                r.get("content_type") or "",
                r.get("meta_robots") or "",
                r.get("x_robots_tag") or "",
                "1" if r.get("indexable") else "0",
                r.get("index_reason") or "",
                r.get("body_internal_link_count") or 0,
                "1" if r.get("has_body_internal_links") else "0",
            ]
        )

    return current_app.response_class(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=pages.csv"},
    )


def _export_filtered_pages_csv(filename: str, filters: DashboardFilters):
    import csv
    import io

    state = load_state()
    rows, _links = _filtered_pages(state, filters)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "url",
            "canonical_url",
            "status_code",
            "content_type",
            "meta_robots",
            "x_robots_tag",
            "indexable",
            "index_reason",
            "body_internal_link_count",
            "has_body_internal_links",
        ]
    )
    for r in rows:
        w.writerow(
            [
                r.get("url", ""),
                r.get("canonical_url") or "",
                r.get("status_code") or "",
                r.get("content_type") or "",
                r.get("meta_robots") or "",
                r.get("x_robots_tag") or "",
                "1" if r.get("indexable") else "0",
                r.get("index_reason") or "",
                r.get("body_internal_link_count") or 0,
                "1" if r.get("has_body_internal_links") else "0",
            ]
        )

    return current_app.response_class(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@bp.get("/export/indexable.csv")
def export_indexable_csv():
    return _export_filtered_pages_csv(
        "indexable.csv",
        DashboardFilters(q="", index="indexable", links="", anchor=""),
    )


@bp.get("/export/non_indexable.csv")
def export_non_indexable_csv():
    return _export_filtered_pages_csv(
        "non_indexable.csv",
        DashboardFilters(q="", index="non_indexable", links="", anchor=""),
    )


@bp.get("/export/with_body_links.csv")
def export_with_body_links_csv():
    return _export_filtered_pages_csv(
        "with_body_links.csv",
        DashboardFilters(q="", index="", links="has", anchor=""),
    )


@bp.get("/export/no_body_links.csv")
def export_no_body_links_csv():
    return _export_filtered_pages_csv(
        "no_body_links.csv",
        DashboardFilters(q="", index="", links="none", anchor=""),
    )


@bp.get("/links")
def page_links():
    """
    Show ALL body-only internal links (with anchor text) for one source page.
    """
    url = (request.args.get("url") or "").strip()
    state = load_state()
    all_links: List[Dict[str, object]] = list(state.get("links") or [])

    items = [l for l in all_links if str(l.get("from_page_url", "")) == url]
    items.sort(key=lambda l: (str(l.get("to_url", "")), str(l.get("anchor_text", ""))))

    # Simple inline HTML (avoid extra template for now)
    out = [
        "<!doctype html><meta charset='utf-8'/>"
        "<meta name='robots' content='noindex,nofollow,noarchive'/>"
        "<title>Audit links</title>"
        "<style>body{font-family:system-ui,Segoe UI,Arial;margin:20px} code{background:#f3f4f6;padding:2px 6px;border-radius:6px} table{width:100%;border-collapse:collapse} td,th{border-bottom:1px solid #ddd;padding:8px;font-size:13px;vertical-align:top} th{color:#555;text-align:left}</style>"
    ]
    out.append(f"<h2>Body internal links</h2><div>From: <code>{url}</code></div>")
    out.append(f"<div style='margin:10px 0'><a href='/audit/'>← Back</a></div>")
    out.append(f"<div>Total links: <b>{len(items)}</b></div>")
    out.append("<table><thead><tr><th>To URL</th><th>Anchor text</th></tr></thead><tbody>")
    for it in items:
        to = str(it.get('to_url', ''))
        anchor = (it.get('anchor_text') or '')
        out.append(f"<tr><td><a href='{to}' target='_blank' rel='noreferrer'>{to}</a></td><td>{anchor}</td></tr>")
    out.append("</tbody></table>")
    return "\\n".join(out)


@bp.get("/export/anchors.csv")
def export_anchors_csv():
    import csv
    import io

    state = load_state()
    q = list(state.get("links") or [])

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["from_page_url", "to_url", "anchor_text"])
    for r in q:
        w.writerow([r.get("from_page_url", ""), r.get("to_url", ""), r.get("anchor_text") or ""])

    return current_app.response_class(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=anchors.csv"},
    )


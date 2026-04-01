from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

from flask import Blueprint, abort, current_app, jsonify, redirect, render_template, request, url_for

from .auth import require_basic_auth
from .crawler import resolve_page_index_explanation
from .service import get_target_base_url, set_target_base_url, start_crawl_async
from .storage import (
    clear_all_run_history,
    delete_archived_run,
    list_run_history,
    load_archived_run,
    load_state,
)

bp = Blueprint("routes", __name__)

PORTAL_VERSION = "2.3.7"


@bp.app_template_filter("index_explain")
def _index_explain_filter(page: Dict[str, Any]) -> str:
    return resolve_page_index_explanation(page)


def _state_for_request() -> Dict[str, Any]:
    rid = request.args.get("run", type=int)
    if rid:
        archived = load_archived_run(rid)
        if not archived:
            abort(404)
        return archived
    return load_state()


def _run_qs() -> str:
    rid = request.args.get("run", type=int)
    return f"?run={rid}" if rid else ""


@bp.context_processor
def _inject_run_context() -> Dict[str, Any]:
    rid = request.args.get("run", type=int)
    live = load_state()
    run = live.get("run") or {}
    st = current_app.config["AUDIT_SETTINGS"]
    tgt = (live.get("target_base_url") or "").strip().rstrip("/")
    if not tgt:
        tgt = (st.base_url or "").strip().rstrip("/")
    return {
        "archived_run_id": rid,
        "viewing_archived": bool(rid),
        "run_qs": _run_qs(),
        "portal_version": PORTAL_VERSION,
        "latest_run": run,
        "target_base_url": tgt,
        "settings": st,
    }


@bp.before_request
def _auth_guard():
    if request.path.rstrip("/").endswith("/api/health"):
        return None
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
    preset: str  # quality / HTTP bucket shortcut; see PRESET_LABELS


def _display_title_str(p: Dict[str, Any]) -> str:
    return (str(p.get("display_title") or p.get("title") or "")).strip()


def _display_desc_str(p: Dict[str, Any]) -> str:
    return (str(p.get("display_description") or p.get("meta_description") or "")).strip()


def _http_status_bucket(p: Dict[str, Any]) -> str:
    sc = p.get("status_code")
    sci: int | None = None
    if sc is not None:
        try:
            sci = int(sc)
        except (TypeError, ValueError):
            sci = None
    if sci is None and sc is None:
        return "none"
    if sci is None:
        return "other"
    if 200 <= sci < 300:
        return "2xx"
    if 300 <= sci < 400:
        return "3xx"
    if 400 <= sci < 500:
        return "4xx"
    if 500 <= sci < 600:
        return "5xx"
    return "other"


PRESET_LABELS: Dict[str, str] = {
    "missing_title": "Missing SEO title",
    "missing_description": "Missing meta / OG / Twitter description",
    "title_long": "Long titles (>60 chars)",
    "desc_long": "Long descriptions (>160 chars)",
    "http_2xx": "HTTP 2xx",
    "http_3xx": "HTTP 3xx",
    "http_4xx": "HTTP 4xx",
    "http_5xx": "HTTP 5xx",
    "http_none": "No HTTP response",
    "http_other": "Other HTTP status",
}

PRESET_BROWSER_ORDER: Tuple[str, ...] = (
    "missing_title",
    "missing_description",
    "title_long",
    "desc_long",
    "http_2xx",
    "http_3xx",
    "http_4xx",
    "http_5xx",
    "http_none",
    "http_other",
)


_PAGE_CSV_HEADER = [
    "url",
    "title_tag",
    "display_title",
    "title_source",
    "h1",
    "meta_description_tag",
    "display_description",
    "description_source",
    "og_title",
    "og_description",
    "twitter_title",
    "twitter_description",
    "canonical_url",
    "status_code",
    "response_time_ms",
    "content_type",
    "meta_robots",
    "x_robots_tag",
    "indexable",
    "index_reason",
    "index_explanation",
    "body_internal_link_count",
    "has_body_internal_links",
]


def _page_csv_row(r: Dict[str, Any]) -> List[Any]:
    disp_t = r.get("display_title") or r.get("title") or ""
    disp_d = r.get("display_description") or r.get("meta_description") or ""
    return [
        r.get("url", ""),
        r.get("title") or "",
        disp_t,
        r.get("title_source") or "",
        r.get("h1") or "",
        r.get("meta_description") or "",
        disp_d,
        r.get("description_source") or "",
        r.get("og_title") or "",
        r.get("og_description") or "",
        r.get("twitter_title") or "",
        r.get("twitter_description") or "",
        r.get("canonical_url") or "",
        r.get("status_code") or "",
        r.get("response_time_ms") if r.get("response_time_ms") is not None else "",
        r.get("content_type") or "",
        r.get("meta_robots") or "",
        r.get("x_robots_tag") or "",
        "1" if r.get("indexable") else "0",
        r.get("index_reason") or "",
        r.get("index_explanation") or resolve_page_index_explanation(dict(r)),
        r.get("body_internal_link_count") or 0,
        "1" if r.get("has_body_internal_links") else "0",
    ]


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

        def _matches_q(p: Dict[str, object]) -> bool:
            if qneedle in str(p.get("url", "")).lower():
                return True
            dt = str(p.get("display_title") or p.get("title") or "").lower()
            if qneedle in dt:
                return True
            dd = str(p.get("display_description") or p.get("meta_description") or "").lower()
            if qneedle in dd:
                return True
            return False

        filtered = [p for p in filtered if _matches_q(p)]
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

    pr = (filters.preset or "").strip()
    if pr == "missing_title":
        filtered = [p for p in filtered if not _display_title_str(p)]
    elif pr == "missing_description":
        filtered = [p for p in filtered if not _display_desc_str(p)]
    elif pr == "title_long":
        filtered = [p for p in filtered if len(_display_title_str(p)) > 60]
    elif pr == "desc_long":
        filtered = [p for p in filtered if len(_display_desc_str(p)) > 160]
    elif pr.startswith("http_"):
        bucket_map = {
            "http_2xx": "2xx",
            "http_3xx": "3xx",
            "http_4xx": "4xx",
            "http_5xx": "5xx",
            "http_none": "none",
            "http_other": "other",
        }
        want = bucket_map.get(pr)
        if want is not None:
            filtered = [p for p in filtered if _http_status_bucket(p) == want]

    filtered.sort(key=lambda p: str(p.get("url", "")))
    return filtered, all_links


def _meta_quality_stats(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    missing_title = sum(1 for p in pages if not _display_title_str(p))
    missing_desc = sum(1 for p in pages if not _display_desc_str(p))
    title_long = sum(1 for p in pages if len(_display_title_str(p)) > 60)
    desc_long = sum(1 for p in pages if len(_display_desc_str(p)) > 160)
    return {
        "missing_title": missing_title,
        "missing_meta_description": missing_desc,
        "title_over_60": title_long,
        "description_over_160": desc_long,
    }


def _dash_url(filters: DashboardFilters, run_id: int | None, **extra: Any) -> str:
    args: Dict[str, Any] = {}
    if filters.q:
        args["q"] = filters.q
    if filters.links:
        args["links"] = filters.links
    if filters.anchor:
        args["anchor"] = filters.anchor
    if filters.index:
        args["index"] = filters.index
    if filters.preset:
        args["preset"] = filters.preset
    args.update(extra)
    if run_id is not None:
        args["run"] = run_id
    return url_for("routes.dashboard", **args)


def _http_status_distribution(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    order = ("2xx", "3xx", "4xx", "5xx", "none", "other")
    counts = {k: 0 for k in order}
    for p in pages:
        counts[_http_status_bucket(p)] += 1
    total = sum(counts.values()) or 1
    colors = {
        "2xx": "#34d399",
        "3xx": "#38bdf8",
        "4xx": "#f87171",
        "5xx": "#dc2626",
        "none": "#64748b",
        "other": "#a78bfa",
    }
    labels = {
        "2xx": "2xx success",
        "3xx": "3xx redirect",
        "4xx": "4xx client error",
        "5xx": "5xx server error",
        "none": "No HTTP response",
        "other": "Other status",
    }
    segments: List[Dict[str, Any]] = []
    for k in order:
        c = counts[k]
        if c == 0:
            continue
        segments.append(
            {
                "key": k,
                "label": labels[k],
                "count": c,
                "pct": round(100.0 * c / total, 1),
                "color": colors[k],
                "preset": f"http_{k}",
            }
        )
    return {"segments": segments, "total": len(pages), "counts": counts}


def _health_score(pages: List[Dict[str, Any]], stats: Dict[str, Any]) -> Dict[str, Any]:
    n = len(pages)
    if n == 0:
        return {
            "score": None,
            "grade": "—",
            "tone": "muted",
            "summary": "Run a crawl to compute a snapshot score from HTTP health, indexability, and title coverage.",
        }
    def _ok_http(sc: Any) -> bool:
        if sc is None:
            return False
        try:
            i = int(sc)
        except (TypeError, ValueError):
            return False
        return 200 <= i < 400

    ok_http = sum(1 for p in pages if _ok_http(p.get("status_code")))
    indexable = int(stats.get("indexable") or 0)
    missing_title = int(stats.get("missing_title") or 0)
    titled = n - missing_title
    score = int(round(100.0 * (0.36 * ok_http / n + 0.36 * indexable / n + 0.28 * titled / n)))
    score = max(0, min(100, score))
    if score >= 82:
        grade, tone = "A", "good"
        label = "Strong"
    elif score >= 68:
        grade, tone = "B", "good"
        label = "Good"
    elif score >= 52:
        grade, tone = "C", "warn"
        label = "Fair"
    elif score >= 35:
        grade, tone = "D", "warn"
        label = "Weak"
    else:
        grade, tone = "F", "bad"
        label = "Critical"
    return {
        "score": score,
        "grade": grade,
        "label": label,
        "tone": tone,
        "summary": f"Weighted mix: HTTP 2xx–3xx success ({ok_http}/{n}), indexable URLs ({indexable}/{n}), and pages with any title signal ({titled}/{n}). Not a Google score—internal crawl snapshot only.",
    }


def _dashboard_insights(
    stats: Dict[str, Any],
    filters: DashboardFilters,
    run_id: int | None,
    http_counts: Dict[str, int],
) -> List[Dict[str, Any]]:
    n = int(stats.get("total_pages") or 0)
    if n <= 0:
        return []
    out: List[Dict[str, Any]] = []

    def add(sev: str, title: str, detail: str, href: str, action: str = "Open") -> None:
        out.append({"severity": sev, "title": title, "detail": detail, "href": href, "action": action})

    ni = int(stats.get("non_indexable") or 0)
    if ni > 0:
        add(
            "critical",
            "Indexing risk",
            f"{ni} of {n} URLs look non-indexable (HTTP errors and/or noindex). Fix high-traffic pages first.",
            _dash_url(filters, run_id, index="non_indexable"),
        )
    c4 = int(http_counts.get("4xx") or 0)
    if c4 > 0:
        add(
            "critical",
            "Client errors (4xx)",
            f"{c4} URLs return 4xx responses—often not indexed and bad for users.",
            _dash_url(filters, run_id, preset="http_4xx"),
        )
    c5 = int(http_counts.get("5xx") or 0)
    if c5 > 0:
        add(
            "critical",
            "Server errors (5xx)",
            f"{c5} URLs returned 5xx—investigate stability and crawl budget waste.",
            _dash_url(filters, run_id, preset="http_5xx"),
        )
    nr = int(http_counts.get("none") or 0)
    if nr > 0:
        add(
            "critical",
            "Failed fetches",
            f"{nr} URLs had no HTTP response (timeout, block, or network).",
            _dash_url(filters, run_id, preset="http_none"),
        )
    mt = int(stats.get("missing_title") or 0)
    if mt > 0:
        add(
            "warning",
            "Missing titles",
            f"{mt} pages have no title tag and no og/twitter title—SERP titles may be poor.",
            _dash_url(filters, run_id, preset="missing_title"),
            "View list",
        )
    md = int(stats.get("missing_meta_description") or 0)
    if md > 0:
        add(
            "warning",
            "Missing descriptions",
            f"{md} pages lack meta / OG / Twitter description text.",
            _dash_url(filters, run_id, preset="missing_description"),
            "View list",
        )
    tl = int(stats.get("title_over_60") or 0)
    dl = int(stats.get("description_over_160") or 0)
    if tl > 0:
        add(
            "info",
            "Long titles (>60 chars)",
            f"{tl} titles may truncate in SERPs.",
            _dash_url(filters, run_id, preset="title_long"),
            "View list",
        )
    if dl > 0:
        add(
            "info",
            "Long descriptions (>160 chars)",
            f"{dl} descriptions exceed a common snippet band.",
            _dash_url(filters, run_id, preset="desc_long"),
            "View list",
        )
    low_ix = n - int(stats.get("indexable") or 0)
    if low_ix == 0 and not out:
        add(
            "info",
            "All URLs indexable (heuristic)",
            "No HTTP/noindex blocks detected in this crawl snapshot.",
            _dash_url(filters, run_id, index="indexable"),
        )
    return out[:10]


def _response_time_stats(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    times = [int(p.get("response_time_ms")) for p in pages if p.get("response_time_ms") is not None]
    if not times:
        return {"avg_ms": None, "p95_ms": None}
    times_sorted = sorted(times)
    n = len(times_sorted)
    avg = sum(times_sorted) // n
    p95_idx = min(n - 1, max(0, int((n - 1) * 0.95)))
    p95 = times_sorted[p95_idx]
    return {"avg_ms": avg, "p95_ms": p95}


@bp.get("/")
def dashboard():
    settings = current_app.config["AUDIT_SETTINGS"]
    state = _state_for_request()
    target_from_state = (state.get("target_base_url") or "").strip()
    if request.args.get("run", type=int):
        target_base_url = target_from_state
    else:
        target_base_url = target_from_state or get_target_base_url()

    run = state.get("run") or {}

    filters = DashboardFilters(
        q=(request.args.get("q") or "").strip(),
        index=(request.args.get("index") or "").strip(),
        links=(request.args.get("links") or "").strip(),
        anchor=(request.args.get("anchor") or "").strip(),
        preset=(request.args.get("preset") or "").strip(),
    )

    all_pages: List[Dict[str, object]] = list(state.get("pages") or [])
    all_links: List[Dict[str, object]] = list(state.get("links") or [])

    stats = {
        "total_pages": len(all_pages),
        "indexable": sum(1 for p in all_pages if p.get("indexable") is True),
        "non_indexable": sum(1 for p in all_pages if p.get("indexable") is False),
        "with_internal_links": sum(1 for p in all_pages if p.get("has_body_internal_links") is True),
    }
    stats.update(_response_time_stats(all_pages))
    stats.update(_meta_quality_stats(all_pages))

    all_pages_ns: List[Dict[str, Any]] = [dict(p) for p in all_pages]
    http_dist = _http_status_distribution(all_pages_ns)
    health = _health_score(all_pages_ns, stats)
    run_id_param: int | None = request.args.get("run", type=int)
    insights = _dashboard_insights(stats, filters, run_id_param, http_dist["counts"])

    filtered, all_links_for_filter = _filtered_pages(state, filters)
    pages = filtered[:500]

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
        link = "/audit/links?url=" + quote(url, safe="")
        rid = request.args.get("run", type=int)
        if rid:
            link += "&run=" + str(rid)
        p["audit_links_url"] = link

    def _export_kw() -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        if filters.q:
            d["q"] = filters.q
        if filters.index:
            d["index"] = filters.index
        if filters.links:
            d["links"] = filters.links
        if filters.anchor:
            d["anchor"] = filters.anchor
        if filters.preset:
            d["preset"] = filters.preset
        if run_id_param is not None:
            d["run"] = run_id_param
        return d

    export_filtered_href = url_for("routes.export_filtered_pages_csv", **_export_kw())
    active_preset_label = PRESET_LABELS.get(filters.preset, "") if filters.preset else ""

    return render_template(
        "dashboard.html",
        settings=settings,
        target_base_url=target_base_url,
        latest_run=run,
        filters=filters,
        pages=pages,
        stats=stats,
        filtered_count=len(filtered),
        http_dist=http_dist,
        health=health,
        insights=insights,
        preset_labels=PRESET_LABELS,
        preset_browser_order=PRESET_BROWSER_ORDER,
        export_filtered_href=export_filtered_href,
        active_preset_label=active_preset_label,
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

    state = _state_for_request()
    rows = list(state.get("pages") or [])

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(_PAGE_CSV_HEADER)
    for r in rows:
        w.writerow(_page_csv_row(dict(r)))

    return current_app.response_class(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=pages.csv"},
    )


def _export_filtered_pages_csv(filename: str, filters: DashboardFilters):
    import csv
    import io

    state = _state_for_request()
    rows, _links = _filtered_pages(state, filters)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(_PAGE_CSV_HEADER)
    for r in rows:
        w.writerow(_page_csv_row(dict(r)))

    return current_app.response_class(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@bp.get("/export/indexable.csv")
def export_indexable_csv():
    return _export_filtered_pages_csv(
        "indexable.csv",
        DashboardFilters(q="", index="indexable", links="", anchor="", preset=""),
    )


@bp.get("/export/non_indexable.csv")
def export_non_indexable_csv():
    return _export_filtered_pages_csv(
        "non_indexable.csv",
        DashboardFilters(q="", index="non_indexable", links="", anchor="", preset=""),
    )


@bp.get("/export/with_body_links.csv")
def export_with_body_links_csv():
    return _export_filtered_pages_csv(
        "with_body_links.csv",
        DashboardFilters(q="", index="", links="has", anchor="", preset=""),
    )


@bp.get("/export/no_body_links.csv")
def export_no_body_links_csv():
    return _export_filtered_pages_csv(
        "no_body_links.csv",
        DashboardFilters(q="", index="", links="none", anchor="", preset=""),
    )


@bp.get("/export/filtered-pages.csv")
def export_filtered_pages_csv():
    filters = DashboardFilters(
        q=(request.args.get("q") or "").strip(),
        index=(request.args.get("index") or "").strip(),
        links=(request.args.get("links") or "").strip(),
        anchor=(request.args.get("anchor") or "").strip(),
        preset=(request.args.get("preset") or "").strip(),
    )
    return _export_filtered_pages_csv("filtered_pages.csv", filters)


@bp.get("/history")
def run_history():
    return render_template("history.html", runs=list_run_history())


@bp.post("/history/delete/<int:run_id>")
def history_delete_run(run_id: int):
    delete_archived_run(run_id)
    return redirect(url_for("routes.run_history"))


@bp.post("/history/clear")
def history_clear_all():
    clear_all_run_history()
    return redirect(url_for("routes.run_history"))


@bp.get("/settings")
def settings_page():
    s = current_app.config["AUDIT_SETTINGS"]
    return render_template("settings.html", settings=s)


@bp.get("/api/health")
def api_health():
    return jsonify(ok=True, version=PORTAL_VERSION, service="audit-portal")


@bp.get("/api/run-status")
def api_run_status():
    state = load_state()
    run = state.get("run") or {}
    st = current_app.config["AUDIT_SETTINGS"]
    return jsonify(
        run=run,
        target_base_url=(state.get("target_base_url") or "").strip(),
        max_pages=int(st.crawl_page_cap()),
    )


@bp.get("/links")
def page_links():
    url = (request.args.get("url") or "").strip()
    state = _state_for_request()
    all_links: List[Dict[str, object]] = list(state.get("links") or [])

    items = [l for l in all_links if str(l.get("from_page_url", "")) == url]
    items.sort(key=lambda l: (str(l.get("to_url", "")), str(l.get("anchor_text", ""))))

    return render_template(
        "links.html",
        page_url=url,
        items=items,
        run_id=request.args.get("run", type=int),
    )


@bp.get("/export/anchors.csv")
def export_anchors_csv():
    import csv
    import io

    state = _state_for_request()
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


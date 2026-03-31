"""One-off smoke checks; run: python -m audit_portal.smoke_test"""
from __future__ import annotations

import os

from audit_portal import create_app
from audit_portal.config import Settings
from audit_portal.crawler import (
    extract_body_internal_links,
    extract_next_data_urls,
    extract_page_meta,
    format_index_explanation,
    is_indexable,
    normalize_url,
    resolve_page_index_explanation,
)
from audit_portal.routes import (
    DashboardFilters,
    _dashboard_insights,
    _health_score,
    _http_status_distribution,
    _meta_quality_stats,
)
from audit_portal.wsgi import app as wsgi_app


def main() -> None:
    checks: list[tuple[str, bool, str]] = []

    def ok(name: str, cond: bool, detail: str = "") -> None:
        checks.append((name, cond, detail))

    assert wsgi_app is not None
    app = create_app()
    st0 = app.config["AUDIT_SETTINGS"]
    ok("crawl_page_cap local", st0.crawl_page_cap() == st0.max_pages, str(st0.crawl_page_cap()))
    os.environ["VERCEL"] = "1"
    st1 = Settings.from_env()
    ok(
        "crawl_page_cap vercel clamp",
        st1.crawl_page_cap() == min(st1.max_pages, max(1, st1.vercel_max_pages)),
        f"{st1.crawl_page_cap()} vs max {st1.max_pages}",
    )
    os.environ.pop("VERCEL", None)
    c = app.test_client()

    r = c.get("/")
    ok("GET / redirects", r.status_code in (301, 302, 303, 307, 308), str(r.status_code))

    r = c.get("/audit/")
    ok("GET /audit/", r.status_code == 200, str(r.status_code))
    ok("dashboard body", b"Audit Portal" in r.data or b"audit" in r.data.lower(), str(len(r.data)))

    r = c.get("/audit/history")
    ok("GET /audit/history", r.status_code == 200, str(r.status_code))

    r = c.get("/audit/settings")
    ok("GET /audit/settings", r.status_code == 200, str(r.status_code))

    r = c.get("/audit/api/health")
    ok("GET /audit/api/health", r.status_code == 200, str(r.get_json()))

    r = c.get("/audit/api/run-status")
    ok("GET /audit/api/run-status", r.status_code == 200, str(r.get_json()))

    r = c.get("/audit/robots.txt")
    ok("GET /audit/robots.txt", r.status_code == 200 and b"Disallow" in r.data, str(r.status_code))

    r = c.get("/audit/export/pages.csv")
    ok("GET pages.csv", r.status_code == 200, r.headers.get("Content-Type", ""))

    html = (
        '<html><head><title>T</title><meta name="description" content="D"/>'
        '</head><body><h1>H</h1><a href="/x">l</a></body></html>'
    )
    m = extract_page_meta(html)
    ok("extract_page_meta title", m.get("display_title") == "T", str(m.get("display_title")))

    idx, reason = is_indexable(200, "", "")
    ok("is_indexable 200", idx and reason == "indexable", reason)

    idx2, r2 = is_indexable(404, "", "")
    ok("is_indexable 404", not idx2, r2)

    ex = format_index_explanation("noindex", meta_robots="noindex", x_robots_tag="")
    ok("format_index_explanation", "noindex" in ex.lower(), ex[:100])

    pages = [
        {"status_code": 200, "indexable": True, "display_title": "a", "display_description": "b"},
        {
            "status_code": 404,
            "indexable": False,
            "display_title": "",
            "display_description": "",
        },
    ]
    dist = _http_status_distribution(pages)
    ok("http_dist has counts", dist["counts"]["2xx"] == 1 and dist["counts"]["4xx"] == 1, str(dist["counts"]))

    stats = {
        "total_pages": 2,
        "indexable": 1,
        "non_indexable": 1,
        **_meta_quality_stats(pages),
    }
    hf = _health_score(pages, stats)
    ok("health score numeric", hf.get("score") is not None, str(hf.get("score")))

    with app.test_request_context("/"):
        ins = _dashboard_insights(stats, DashboardFilters("", "", "", ""), None, dist["counts"])
    ok("insights", isinstance(ins, list) and len(ins) >= 1, str(len(ins)))

    p = {"index_reason": "http_404", "status_code": 404}
    ok("resolve_page_index", len(resolve_page_index_explanation(p)) > 20, "")

    links = extract_body_internal_links(html, "https://ex.com/a", "https://ex.com")
    ok("body internal links", len(links) >= 1, str(links))

    nu = normalize_url("https://ex.com/a/")
    ok("normalize_url", nu.endswith("/a") or "ex.com" in nu, nu)

    nd = (
        '<!doctype html><script id="__NEXT_DATA__" type="application/json">'
        '{"props":{"x":"/segment/about","y":"https://ex.com/abs"}}'
        "</script>"
    )
    nurls = extract_next_data_urls(nd, "https://ex.com/start", "https://ex.com")
    ok("next_data urls", len(nurls) >= 2, repr(nurls))

    ok("jinja filter index_explain", "index_explain" in app.jinja_env.filters, "")

    r = c.get("/audit/?run=999999999")
    ok("archived run missing -> 404", r.status_code == 404, str(r.status_code))

    failed = [x for x in checks if not x[1]]
    print(f"PASSED {len(checks) - len(failed)} / {len(checks)}")
    for name, _, detail in failed:
        print(f"FAIL {name}: {detail}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

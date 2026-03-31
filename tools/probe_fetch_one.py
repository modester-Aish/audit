from __future__ import annotations

import os
import sys

import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from audit_portal.crawler import SimpleCrawler, extract_body_internal_links, normalize_url
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin


def main() -> None:
    base = os.getenv("PROBE_BASE_URL", "https://backend.seordp.net").rstrip("/")
    target = os.getenv("PROBE_TARGET_URL", base + "/")

    crawler = SimpleCrawler(base, max_pages=1)
    res = crawler.fetch(target)

    print("fetch_url:", normalize_url(target))
    print("status_code:", res.status_code)
    print("content_type:", res.content_type)
    print("meta_robots:", res.meta_robots)
    print("x_robots_tag:", res.x_robots_tag)
    print("canonical_url:", res.canonical_url)

    links = extract_body_internal_links(res.html, res.url, base)
    print("body_internal_links_count:", len(links))
    # Show a few anchors for confirmation
    for i, (to_url, anchor) in enumerate(links[:10], start=1):
        print(f"anchor_{i}:", anchor[:80].replace("\n", " ").strip(), "=>", to_url)

    # Debug: how many <a href> exist in body HTML at all?
    soup = BeautifulSoup(res.html, "lxml")
    body = soup.body
    if body:
        all_body_anchors = body.select("a[href]")
        print("body_total_anchors:", len(all_body_anchors))
        # Print a few raw hrefs to understand filtering
        samples = []
        for a in all_body_anchors[:10]:
            href = (a.get("href") or "").strip()
            if not href:
                continue
            samples.append(href[:120])
        print("body_anchor_href_samples:", samples)
        # Estimate internal links by host match only
        base_host = urlparse(base).netloc.lower()
        internal_est = 0
        for a in all_body_anchors:
            href = (a.get("href") or "").strip()
            if not href:
                continue
            abs_url = urljoin(res.url, href)
            if urlparse(abs_url).netloc.lower() == base_host:
                internal_est += 1
        print("body_internal_links_est_by_host:", internal_est)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("probe_fetch_one_error:", str(e))
        sys.exit(1)


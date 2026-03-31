from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urldefrag, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


_WHITESPACE_RE = re.compile(r"\s+")


def normalize_url(url: str) -> str:
    url = url.strip()
    url, _frag = urldefrag(url)
    parsed = urlparse(url)

    # Normalize scheme/host case, remove default ports, keep path + query.
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    path = parsed.path or "/"
    # Drop trailing slash duplicates except root
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def is_http_url(url: str) -> bool:
    scheme = urlparse(url).scheme.lower()
    return scheme in {"http", "https"}


def same_site(url: str, base: str) -> bool:
    def host(u: str) -> str:
        p = urlparse(u)
        h = (p.hostname or "").lower()
        if h.startswith("www."):
            h = h[4:]
        return h

    # Treat www/non-www as same site; don't require scheme match (http/https).
    return host(url) == host(base)


def extract_meta_robots(soup: BeautifulSoup) -> str:
    # Combine all <meta name="robots" content="..."> values.
    values: List[str] = []
    for meta in soup.select('meta[name="robots"]'):
        content = (meta.get("content") or "").strip()
        if content:
            values.append(content)
    return ", ".join(values) if values else ""


def parse_x_robots_tag(header_value: str) -> str:
    return (header_value or "").strip()


def is_indexable(status_code: Optional[int], meta_robots: str, x_robots_tag: str) -> Tuple[bool, str]:
    if status_code is None:
        return False, "no_response"
    if status_code >= 400:
        return False, f"http_{status_code}"
    if status_code in {204, 304}:
        return False, f"http_{status_code}"

    directives = ",".join([meta_robots or "", x_robots_tag or ""]).lower()
    if "noindex" in directives:
        return False, "noindex"
    return True, "indexable"


def _strip_layout_sections(soup: BeautifulSoup) -> BeautifulSoup:
    # Remove common non-body sections before link extraction.
    for sel in ["header", "footer", "nav", "aside"]:
        for el in soup.select(sel):
            el.decompose()

    # Keep this intentionally minimal to avoid removing anchors from main content.
    # If needed later, we can add more specific selectors based on observed HTML.
    for el in soup.select('[role="navigation"]'):
        el.decompose()

    return soup


def extract_body_internal_links(html: str, page_url: str, base_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    body = soup.body
    if not body:
        return []

    body_soup = BeautifulSoup(str(body), "lxml")
    _strip_layout_sections(body_soup)

    out: List[Tuple[str, str]] = []
    for a in body_soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        if not is_http_url(abs_url):
            continue
        abs_url = normalize_url(abs_url)
        if not same_site(abs_url, base_url):
            continue

        anchor = a.get_text(" ", strip=True) or ""
        anchor = _WHITESPACE_RE.sub(" ", anchor).strip()
        out.append((abs_url, anchor))
    return out


def extract_all_internal_links(html: str, page_url: str, base_url: str) -> Set[str]:
    # Used for discovery crawl (includes header/footer), while body-only is stored separately.
    soup = BeautifulSoup(html, "lxml")
    urls: Set[str] = set()
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        if not is_http_url(abs_url):
            continue
        abs_url = normalize_url(abs_url)
        if same_site(abs_url, base_url):
            urls.add(abs_url)
    return urls


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: Optional[int]
    content_type: str
    meta_robots: str
    x_robots_tag: str
    canonical_url: str
    html: str


class SimpleCrawler:
    def __init__(
        self,
        base_url: str,
        *,
        max_pages: int = 5000,
        request_timeout_seconds: int = 20,
        user_agent: str = "SEO-Audit-Portal/1.0",
        crawl_delay_seconds: float = 0.0,
    ) -> None:
        self.base_url = normalize_url(base_url.rstrip("/"))
        self.max_pages = max_pages
        self.request_timeout_seconds = request_timeout_seconds
        self.user_agent = user_agent
        self.crawl_delay_seconds = crawl_delay_seconds

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml"})

    def fetch(self, url: str) -> FetchResult:
        url = normalize_url(url)
        status_code: Optional[int] = None
        content_type = ""
        x_robots_tag = ""
        html = ""
        canonical = ""
        meta_robots = ""

        try:
            resp = self.session.get(url, timeout=self.request_timeout_seconds, allow_redirects=True)
            status_code = resp.status_code
            content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            x_robots_tag = parse_x_robots_tag(resp.headers.get("X-Robots-Tag", ""))

            if content_type.startswith("text/html") and resp.text:
                html = resp.text
                soup = BeautifulSoup(html, "lxml")
                meta_robots = extract_meta_robots(soup)
                link = soup.select_one('link[rel="canonical"][href]')
                if link:
                    canonical = (link.get("href") or "").strip()
                    if canonical:
                        canonical = normalize_url(urljoin(url, canonical))
        except Exception:
            # Keep defaults for error cases.
            pass

        return FetchResult(
            url=url,
            status_code=status_code,
            content_type=content_type,
            meta_robots=meta_robots,
            x_robots_tag=x_robots_tag,
            canonical_url=canonical,
            html=html,
        )

    def crawl(self) -> Iterable[FetchResult]:
        if not self.base_url:
            return []

        seen: Set[str] = set()
        queue: List[str] = [self.base_url]

        while queue and len(seen) < self.max_pages:
            url = queue.pop(0)
            url = normalize_url(url)
            if url in seen:
                continue
            if not same_site(url, self.base_url):
                continue
            seen.add(url)

            if self.crawl_delay_seconds > 0:
                time.sleep(self.crawl_delay_seconds)

            result = self.fetch(url)
            yield result

            # Discover more URLs only from HTML pages
            if result.html and (result.content_type.startswith("text/html") or result.content_type == ""):
                discovered = extract_all_internal_links(result.html, result.url, self.base_url)
                for d in discovered:
                    if d not in seen:
                        queue.append(d)


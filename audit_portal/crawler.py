from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urldefrag, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


_WHITESPACE_RE = re.compile(r"\s+")


def _meta_name_ci(soup: BeautifulSoup, name: str) -> Optional[str]:
    want = name.lower()
    for meta in soup.select("meta[name]"):
        if ((meta.get("name") or "").strip().lower() == want) and meta.get("content") is not None:
            v = (meta.get("content") or "").strip()
            if v:
                return v
    return None


def _meta_property_ci(soup: BeautifulSoup, prop: str) -> Optional[str]:
    want = prop.lower()
    for meta in soup.select("meta[property]"):
        if ((meta.get("property") or "").strip().lower() == want) and meta.get("content") is not None:
            v = (meta.get("content") or "").strip()
            if v:
                return v
    return None


def _itemprop_description(soup: BeautifulSoup) -> Optional[str]:
    for meta in soup.select('meta[itemprop="description"]'):
        v = (meta.get("content") or "").strip()
        if v:
            return v
    return None


def extract_page_meta(html: str) -> Dict[str, Any]:
    """
    Extract title tag, meta description, Open Graph & Twitter fallbacks, H1.
    display_* fields prefer <title> / standard meta, then og / twitter (common for SPAs
    that only set social tags). See also: Open Graph / Twitter Card tag practices.
    """
    soup = BeautifulSoup(html, "lxml")
    title_el = soup.title
    title_tag = title_el.get_text(strip=True) if title_el else None
    if title_tag == "":
        title_tag = None

    meta_desc = _meta_name_ci(soup, "description") or _itemprop_description(soup)
    og_title = _meta_property_ci(soup, "og:title")
    og_desc = _meta_property_ci(soup, "og:description")
    tw_title = _meta_name_ci(soup, "twitter:title")
    tw_desc = _meta_name_ci(soup, "twitter:description")

    h1_el = soup.select_one("h1")
    h1 = h1_el.get_text(" ", strip=True) if h1_el else None
    if h1 == "":
        h1 = None

    display_title = title_tag or og_title or tw_title
    title_source: Optional[str] = None
    if title_tag:
        title_source = "title"
    elif og_title:
        title_source = "og"
    elif tw_title:
        title_source = "twitter"

    display_description = meta_desc or og_desc or tw_desc
    desc_source: Optional[str] = None
    if meta_desc:
        desc_source = "meta"
    elif og_desc:
        desc_source = "og"
    elif tw_desc:
        desc_source = "twitter"

    return {
        "title": title_tag,
        "meta_description": meta_desc,
        "og_title": og_title,
        "og_description": og_desc,
        "twitter_title": tw_title,
        "twitter_description": tw_desc,
        "h1": h1,
        "display_title": display_title,
        "display_description": display_description,
        "title_source": title_source,
        "description_source": desc_source,
    }


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


_HTTP_WHY: Dict[int, str] = {
    204: "HTTP 204 - no content body",
    301: "HTTP 301 - moved permanently (unusual as final after redirects)",
    302: "HTTP 302 - found / temporary redirect (unusual as final)",
    304: "HTTP 304 - not modified (no body to index)",
    400: "HTTP 400 - bad request",
    401: "HTTP 401 - unauthorized (login required)",
    403: "HTTP 403 - forbidden (crawlers or guests blocked)",
    404: "HTTP 404 - not found",
    405: "HTTP 405 - method not allowed",
    408: "HTTP 408 - request timeout",
    410: "HTTP 410 - gone (removal signal)",
    429: "HTTP 429 - too many requests (rate limited)",
    500: "HTTP 500 - internal server error",
    502: "HTTP 502 - bad gateway",
    503: "HTTP 503 - service unavailable",
    504: "HTTP 504 - gateway timeout",
}


def format_index_explanation(
    reason_code: str,
    *,
    status_code: Optional[int] = None,
    meta_robots: str = "",
    x_robots_tag: str = "",
) -> str:
    """
    Human-readable sentence: why this URL is treated as indexable or not (heuristic).
    """
    code = (reason_code or "").strip()
    if code == "indexable":
        return (
            "Likely indexable: HTTP response succeeded and no 'noindex' was found in meta robots or "
            "X-Robots-Tag. (On-page rule only; not proof the URL is in any search engine index.)"
        )
    if code == "no_response":
        return (
            "Not indexable: no HTTP response (connection failed, timeout, DNS/SSL error, or blocked fetch). "
            "Search engines normally have nothing successful to index."
        )
    if code.startswith("http_"):
        try:
            sc = int(code.split("_", 1)[1])
        except (ValueError, IndexError):
            sc = status_code
        if sc is not None:
            hint = _HTTP_WHY.get(sc, f"HTTP {sc} - non-success or empty response")
            return f"Not indexable: {hint}. Error or non-content status codes are usually not indexed."
        return "Not indexable: HTTP error or unusual status; typically skipped for indexing."
    if code == "noindex":
        srcs: List[str] = []
        if "noindex" in (meta_robots or "").lower():
            srcs.append("HTML <meta name=\"robots\"> (or equivalent)")
        if "noindex" in (x_robots_tag or "").lower():
            srcs.append("X-Robots-Tag HTTP header")
        if srcs:
            where = " and ".join(srcs)
            verb = "contain" if len(srcs) > 1 else "contains"
            return (
                f"Not indexable: {where} {verb} 'noindex', telling crawlers not to index this URL."
            )
        return "Not indexable: a 'noindex' directive was detected in page or header signals."
    if code:
        return f"Not indexable (code: {code})."
    return "Unknown indexability signal."


def resolve_page_index_explanation(page: Dict[str, Any]) -> str:
    existing = page.get("index_explanation")
    if existing:
        return str(existing)
    sc_raw = page.get("status_code")
    sc: Optional[int]
    if isinstance(sc_raw, int):
        sc = sc_raw
    elif sc_raw is not None and str(sc_raw).isdigit():
        sc = int(str(sc_raw))
    else:
        sc = None
    return format_index_explanation(
        str(page.get("index_reason") or ""),
        status_code=sc,
        meta_robots=str(page.get("meta_robots") or ""),
        x_robots_tag=str(page.get("x_robots_tag") or ""),
    )


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
    response_time_ms: Optional[int]
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

        elapsed_ms: Optional[int] = None
        try:
            resp = self.session.get(url, timeout=self.request_timeout_seconds, allow_redirects=True)
            elapsed_ms = int(resp.elapsed.total_seconds() * 1000)
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
            pass

        return FetchResult(
            url=url,
            status_code=status_code,
            content_type=content_type,
            meta_robots=meta_robots,
            x_robots_tag=x_robots_tag,
            canonical_url=canonical,
            response_time_ms=elapsed_ms,
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


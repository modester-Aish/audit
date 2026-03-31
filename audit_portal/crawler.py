from __future__ import annotations

import json
import re
import time
import xml.etree.ElementTree as ET
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


def _looks_like_html(text: str) -> bool:
    """True if body is likely HTML (including minimal SPAs with __NEXT_DATA__)."""
    if not text or len(text) < 12:
        return False
    head = text.lstrip()[:8000]
    low = head.lower()
    if low.startswith("<!doctype html") or "<html" in low[:800]:
        return True
    if "__next_data__" in low:
        return True
    return False


def extract_sitemap_hrefs_from_html(html: str, page_url: str) -> List[str]:
    """<link rel=\"sitemap\" href=\"...\"> on the homepage."""
    out: List[str] = []
    try:
        soup = BeautifulSoup(html, "lxml")
        for ln in soup.select('link[rel="sitemap"][href]'):
            u = (ln.get("href") or "").strip()
            if not u:
                continue
            abs_u = normalize_url(urldefrag(urljoin(page_url, u))[0])
            if is_http_url(abs_u):
                out.append(abs_u)
    except Exception:
        pass
    return out


def extract_next_data_urls(html: str, page_url: str, base_url: str) -> Set[str]:
    """Harvest same-site URLs embedded in Next.js __NEXT_DATA__ JSON (common when <a> tags are sparse)."""
    out: Set[str] = set()
    try:
        soup = BeautifulSoup(html, "lxml")
        el = soup.select_one("script#__NEXT_DATA__")
        raw = (el.string or el.get_text() or "").strip() if el else ""
        if not raw:
            return out
        data = json.loads(raw)
    except Exception:
        return out

    nodes = 0
    max_nodes = 80_000
    stack: List[Any] = [data]
    while stack and nodes < max_nodes:
        obj = stack.pop()
        nodes += 1
        if isinstance(obj, str):
            s = obj.strip()
            if not s or len(s) > 2048:
                continue
            if s.startswith("/") and not s.startswith("//"):
                path0 = s.split("?", 1)[0].split("#", 1)[0]
                if 1 <= len(path0) <= 2000 and "{" not in path0 and "}" not in path0:
                    u = normalize_url(urljoin(page_url, s.split("#", 1)[0]))
                    if same_site(u, base_url):
                        out.add(u)
            elif is_http_url(s):
                u = normalize_url(urldefrag(s)[0])
                if same_site(u, base_url):
                    out.add(u)
        elif isinstance(obj, dict):
            stack.extend(obj.values())
        elif isinstance(obj, list):
            stack.extend(obj)
    return out


def _xml_local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1].lower()
    return tag.lower()


def _parse_sitemap_root(content: bytes) -> Tuple[bool, List[str]]:
    """Return (is_sitemap_index, loc values)."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return False, []
    root_name = _xml_local_name(root.tag)
    is_index = "sitemapindex" in root_name
    locs: List[str] = []
    for el in root.iter():
        if _xml_local_name(el.tag) == "loc":
            t = (el.text or "").strip()
            if t:
                locs.append(t)
    return is_index, locs


def discover_urls_from_sitemaps(
    base_url: str,
    session: requests.Session,
    *,
    timeout: int,
    max_urls: int,
) -> List[str]:
    """
    Pull URLs from robots.txt Sitemap: lines and common /sitemap.xml paths, including
    nested sitemap indexes. Use alongside HTML link crawling so JS-heavy sites still
    get a full URL list up to max_urls.
    """
    if max_urls <= 0:
        return []
    base = normalize_url(base_url.rstrip("/"))
    roots: List[str] = []
    html_accept = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    try:
        home = session.get(base, timeout=timeout, headers=html_accept)
        if home.status_code == 200 and home.text and _looks_like_html(home.text):
            for h in extract_sitemap_hrefs_from_html(home.text, home.url or base):
                roots.append(h)
    except Exception:
        pass
    try:
        rr = session.get(urljoin(base, "/robots.txt"), timeout=timeout)
        if rr.status_code == 200 and rr.text:
            for line in rr.text.splitlines():
                s = line.strip()
                if s.lower().startswith("sitemap:"):
                    u = s.split(":", 1)[-1].strip()
                    if u and is_http_url(u):
                        roots.append(normalize_url(urldefrag(u)[0]))
    except Exception:
        pass
    for path in (
        "sitemap.xml",
        "sitemap_index.xml",
        "sitemap-index.xml",
        "wp-sitemap.xml",
        "post-sitemap.xml",
        "page-sitemap.xml",
        "product-sitemap.xml",
        "sitemap/sitemap.xml",
    ):
        roots.append(normalize_url(urljoin(base + "/", path)))

    seen_roots: Set[str] = set()
    ordered_roots: List[str] = []
    for r in roots:
        if r not in seen_roots:
            seen_roots.add(r)
            ordered_roots.append(r)

    pages: List[str] = []
    seen_pages: Set[str] = set()
    pending: List[str] = list(ordered_roots)
    pending_set: Set[str] = set(ordered_roots)
    done_maps: Set[str] = set()
    accept = {"Accept": "application/xml, text/xml, application/xhtml+xml, */*;q=0.8"}

    while pending and len(pages) < max_urls:
        sm_raw = pending.pop(0)
        pending_set.discard(sm_raw)
        sm_url = normalize_url(urldefrag(sm_raw)[0])
        if sm_url in done_maps:
            continue
        done_maps.add(sm_url)
        try:
            r = session.get(sm_url, timeout=timeout, headers=accept)
            if r.status_code != 200 or not r.content:
                continue
            is_index, locs = _parse_sitemap_root(r.content)
        except Exception:
            continue
        if is_index:
            for loc in locs:
                u = normalize_url(urldefrag(loc)[0])
                if is_http_url(u) and u not in done_maps and u not in pending_set:
                    pending_set.add(u)
                    pending.append(u)
        else:
            for loc in locs:
                if len(pages) >= max_urls:
                    break
                u = normalize_url(urldefrag(loc)[0])
                if not is_http_url(u) or not same_site(u, base):
                    continue
                if u not in seen_pages:
                    seen_pages.add(u)
                    pages.append(u)

    return pages


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
        use_sitemap_seed: bool = True,
        sitemap_seed_cap: int = 5000,
        try_parse_html_on_error: bool = True,
    ) -> None:
        self.base_url = normalize_url(base_url.rstrip("/"))
        self.max_pages = max_pages
        self.request_timeout_seconds = request_timeout_seconds
        self.user_agent = user_agent
        self.crawl_delay_seconds = crawl_delay_seconds
        self.use_sitemap_seed = use_sitemap_seed
        self.sitemap_seed_cap = max(0, int(sitemap_seed_cap))
        self.try_parse_html_on_error = bool(try_parse_html_on_error)

        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        )

    def _extract_html_body(
        self, raw: str, status_code: Optional[int], content_type: str
    ) -> str:
        if not raw:
            return ""
        ct = (content_type or "").strip().lower()
        if ct.startswith("text/html") or ct.startswith("application/xhtml"):
            return raw
        if _looks_like_html(raw):
            return raw
        if self.try_parse_html_on_error and status_code is not None and status_code >= 400:
            if _looks_like_html(raw):
                return raw
        return ""

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

            raw = resp.text or ""
            html = self._extract_html_body(raw, status_code, content_type)
            if html:
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
        queued: Set[str] = {self.base_url}

        if self.use_sitemap_seed and self.sitemap_seed_cap > 0:
            cap = min(self.max_pages, self.sitemap_seed_cap)
            seeds = discover_urls_from_sitemaps(
                self.base_url,
                self.session,
                timeout=self.request_timeout_seconds,
                max_urls=cap,
            )
            for u in seeds:
                if u != self.base_url and u not in queued:
                    queued.add(u)
                    queue.append(u)

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

            # Discover more URLs from HTML (anchors + Next.js __NEXT_DATA__, even if MIME type is wrong)
            if result.html:
                discovered = extract_all_internal_links(result.html, result.url, self.base_url)
                discovered |= extract_next_data_urls(result.html, result.url, self.base_url)
                for d in discovered:
                    if d not in seen and d not in queued:
                        queued.add(d)
                        queue.append(d)


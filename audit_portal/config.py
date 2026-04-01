from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    port: int
    secret_key: str
    base_url: str
    require_auth: bool
    audit_username: str
    audit_password: str
    auto_recrawl_minutes: int
    max_pages: int
    request_timeout_seconds: int
    user_agent: str
    crawl_delay_seconds: float
    ignore_robots_txt: bool
    try_parse_html_on_error: bool
    use_sitemap_seed: bool
    sitemap_seed_cap: int
    # If true, only URLs from sitemaps (+ homepage seed) are fetched—no new URLs from HTML <a> / __NEXT_DATA__.
    sitemap_only: bool
    # When VERCEL=1, crawls run in the HTTP request (background threads freeze after response).
    vercel_max_pages: int

    @property
    def sqlite_uri(self) -> str:
        # Keep DB inside audit-portal folder
        return "sqlite:///audit_portal.sqlite3"

    @staticmethod
    def from_env() -> "Settings":
        load_dotenv()

        port = int(os.getenv("PORT", "5055"))
        secret_key = os.getenv("SECRET_KEY", "change-me")
        base_url = os.getenv("BASE_URL", "").strip()
        if not base_url:
            # Allow the app to start, but dashboard will prompt to configure it.
            base_url = ""

        # Local default: require auth. On Vercel, .env is usually missing — default off so the app
        # loads unless you set REQUIRE_AUTH=true in the project env.
        _require_auth_default = not bool(os.environ.get("VERCEL"))

        return Settings(
            port=port,
            secret_key=secret_key,
            base_url=base_url.rstrip("/"),
            require_auth=_get_bool("REQUIRE_AUTH", _require_auth_default),
            audit_username=os.getenv("AUDIT_USERNAME", "admin"),
            audit_password=os.getenv("AUDIT_PASSWORD", "change-me"),
            auto_recrawl_minutes=int(os.getenv("AUTO_RECRAWL_MINUTES", "0")),
            max_pages=int(os.getenv("MAX_PAGES", "5000")),
            request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
            user_agent=os.getenv("USER_AGENT", "SEO-Audit-Portal/1.0"),
            crawl_delay_seconds=float(os.getenv("CRAWL_DELAY_SECONDS", "0")),
            ignore_robots_txt=_get_bool("IGNORE_ROBOTS_TXT", True),
            try_parse_html_on_error=_get_bool("TRY_PARSE_HTML_ON_ERROR", True),
            use_sitemap_seed=_get_bool("USE_SITEMAP_SEED", True),
            sitemap_seed_cap=int(os.getenv("SITEMAP_SEED_CAP", "5000")),
            # Default on: crawl sitemap URLs only unless you set SITEMAP_ONLY=false
            sitemap_only=_get_bool("SITEMAP_ONLY", True),
            vercel_max_pages=int(os.getenv("VERCEL_MAX_PAGES", "5000")),
        )

    def crawl_page_cap(self) -> int:
        """Max URLs to fetch this run. On Vercel, clamped so the crawl can finish inside one request."""
        if os.environ.get("VERCEL"):
            return min(self.max_pages, max(1, int(self.vercel_max_pages)))
        return int(self.max_pages)


### Read data from FBREF ##

# Imports 
# fbref.py
"""
FBref scraper utilities (single-file module).

Key points
- Module-level logger (no need to pass logger into every function).
- Handles FBref tables embedded in HTML comments.
- NO sleeping/rate limiting here (do that in your pipeline/orchestrator).
- More browser-like requests + Session + retries to reduce 403.
- Optional cloudscraper fallback for stubborn 403 blocks.

Dependencies:
    pip install pandas requests beautifulsoup4 lxml
Optional:
    pip install cloudscraper

Example usage (elsewhere):
    import fbref

    fbref.configure_logging(level="DEBUG")
    df = fbref.scrape_fbref(
        url="https://fbref.com/en/comps/9/stats/Premier-League-Stats",
        table_id="stats_standard",
        use_cloudscraper_fallback=True,
    )
"""

from __future__ import annotations

import logging
import os
import re
from typing import Optional, List

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import StringIO

# Local imports
from functions.logger import get_logger
from functions.data_related import flatten_columns
from environment.variable import OS_USAGE, OS_PROFILES


# Logging (module-level)

log = get_logger(__name__)

# Function: Set up the logging thorugh the process
def configure_logging(level: str | int = logging.INFO, log_file: str | None = None) -> None:
    """
    Optionally configure this module's logger from the outside.

    Example:
        configure_logging(level="DEBUG")
        configure_logging(level=logging.INFO, log_file="fbref.log")
    """
    global log
    lvl = getattr(logging, level.upper(), level) if isinstance(level, str) else level
    log = get_logger(__name__, level=lvl, log_file=log_file)
    log.debug("fbref.py logger configured")



# --- --- HTTP (Session + headers) --- ---

SESSION = requests.Session()
# Browser-like headers to reduce blocks.
HEADERS = {
    "User-Agent": (OS_PROFILES[OS_USAGE]["ua"]),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Referer": "https://fbref.com/",
}

# Retries for transient issues. No backoff sleeping here (caller handles pacing).
_retry = Retry(
    total=3,
    backoff_factor=0,  # IMPORTANT: keep 0 to avoid hidden sleep here
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry))

# Function: Proxy settings
def _env_proxies() -> Optional[dict]:
    """
    Optional proxy support via environment variables:
        HTTP_PROXY, HTTPS_PROXY
    """
    http_p = os.getenv("HTTP_PROXY")
    https_p = os.getenv("HTTPS_PROXY")
    if not http_p and not https_p:
        return None
    return {"http": http_p, "https": https_p}

# Function: Get the full page to extract all information
def fetch_html(url: str) -> str:
    """
    Fetch raw HTML from FBref using a persistent Session.
    Rate limiting/retries/backoff timing should be handled by caller.

    Raises:
        requests.HTTPError for non-200 responses.
    """
    log.info("Fetching URL: %s", url)
    proxies = _env_proxies()
    if proxies:
        log.warning("Using proxies from environment variables")

    resp = SESSION.get(url, headers=HEADERS, timeout=30, proxies=proxies)

    if resp.status_code == 403:
        log.error(
            "403 Forbidden from FBref. This is usually anti-bot/CDN blocking.\n"
            "Try slower pacing in your main/orchestrator, caching, or enable "
            "use_cloudscraper_fallback=True."
        )

    resp.raise_for_status()
    log.debug("Fetched %d bytes", len(resp.text))
    return resp.text

# Function: Fallback if other one is not possible (anti-bot)
def fetch_html_with_cloudscraper(url: str) -> str:
    """
    Optional fallback that can bypass some anti-bot protections.

    Requires:
        pip install cloudscraper
    """
    try:
        import cloudscraper
    except ImportError as e:
        raise RuntimeError(
            "cloudscraper is not installed. Install it via: pip install cloudscraper"
        ) from e

    log.warning("Using cloudscraper fallback for: %s", url)
    proxies = _env_proxies()
    if proxies:
        log.warning("cloudscraper will use proxies from environment variables (requests-compatible)")

    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": OS_PROFILES[OS_USAGE]["cloudscraper_platform"], "desktop": True}
    )
    resp = scraper.get(url, headers=HEADERS, timeout=30, proxies=proxies)
    resp.raise_for_status()
    log.debug("Fetched %d bytes via cloudscraper", len(resp.text))
    return resp.text



# --- --- Tables: extraction/selection --- ---
# Function: Get all tables at the page
def extract_tables(html: str) -> List[pd.DataFrame]:
    """
    Extract all tables from HTML, including tables inside HTML comments.
    Returns a list of DataFrames.
    """
    tables: List[pd.DataFrame] = []

    # Direct tables
    try:
        direct = pd.read_html(StringIO(html), flavor="lxml")
        tables.extend(direct)
        log.info("Direct tables found: %d", len(direct))
    except ValueError:
        log.warning("No direct tables found")

    # Commented tables
    soup = BeautifulSoup(html, "lxml")
    comments = soup.find_all(string=lambda t: isinstance(t, Comment))

    commented_count = 0
    for c in comments:
        c_str = str(c)
        if "<table" not in c_str:
            continue
        try:
            dfs = pd.read_html(StringIO(c_str), flavor="lxml")
            tables.extend(dfs)
            commented_count += len(dfs)
        except ValueError:
            pass

    log.info("Commented tables found: %d", commented_count)

    if not tables:
        log.error("No tables extracted from HTML (blocked? structure changed?)")
        raise RuntimeError("No tables extracted from HTML")

    return tables

# Function: Extraxt only those tables, we are looking for
def select_table_by_id(html: str, table_id: str) -> pd.DataFrame:
    """
    Select a specific FBref table by its HTML id.
    Works for both normal DOM and commented tables.
    """
    soup = BeautifulSoup(html, "lxml")

    # Normal DOM
    t = soup.find("table", id=table_id)
    if t is not None:
        log.info("Table '%s' found in DOM", table_id)
        return pd.read_html(str(t), flavor="lxml")[0]

    # Commented DOM: find comment that contains this id, then extract that table
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        c_str = str(c)
        if f'id="{table_id}"' not in c_str and f"id='{table_id}'" not in c_str:
            continue

        # Extract the specific table HTML from the comment block
        m = re.search(
            r"(<table[^>]*\bid=['\"]" + re.escape(table_id) + r"['\"][\s\S]*?</table>)",
            c_str,
        )
        if m:
            log.info("Table '%s' found in comments", table_id)
            return pd.read_html(StringIO(m.group(1)), flavor="lxml")[0]

        # Fallback: try parsing entire comment (may work if only one table)
        try:
            log.info(
                "Table id '%s' present in comments; parsing entire comment as fallback",
                table_id,
            )
            return pd.read_html(StringIO(c_str), flavor="lxml")[0]
        except ValueError:
            break

    log.error("Table id '%s' not found", table_id)
    raise ValueError(f"Table id '{table_id}' not found")

# Function: (Optional) get column names
def select_table_by_pattern(tables: List[pd.DataFrame], pattern: str) -> pd.DataFrame:
    """
    Select a table by regex pattern matching column names or a small content sample.
    """
    rx = re.compile(pattern, re.IGNORECASE)

    for i, df in enumerate(tables):
        col_text = " ".join(map(str, df.columns))
        if rx.search(col_text):
            log.info("Matched table %d by columns (pattern=%r)", i, pattern)
            return df

        sample = df.head(3).astype(str).to_string(index=False)
        if rx.search(sample):
            log.info("Matched table %d by content (pattern=%r)", i, pattern)
            return df

    log.error("No table matched pattern %r", pattern)
    raise ValueError(f"No table matched pattern {pattern!r}")



# --- --- Public API --- ---
# Function: Final function combining all with two options
def scrape_fbref(
    url: str,
    table_id: Optional[list] = None,
    match_pattern: Optional[str] = None,
    use_cloudscraper_fallback: bool = False,
) -> pd.DataFrame:
    """
    High-level helper: fetch + select a table into a DataFrame.

    Provide ONE of:
        - table_id: best option (stable)
        - match_pattern: regex fallback

    If you hit 403, set:
        use_cloudscraper_fallback=True

    Example:
        df = scrape_fbref(url, table_id="stats_standard")
        df = scrape_fbref(url, match_pattern=r"Squad|Pts|xG")
    """
    try:
        html = fetch_html(url)
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status == 403 and use_cloudscraper_fallback:
            html = fetch_html_with_cloudscraper(url)
        else:
            raise

    if table_id:
        data = select_table_by_id(html, table_id)
        

    if match_pattern:
        tables = extract_tables(html)
        data  = select_table_by_pattern(tables, match_pattern)

    return flatten_columns(data)

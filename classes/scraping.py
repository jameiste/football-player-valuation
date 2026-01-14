### Scraper for website info ### 

# Imports
from __future__ import annotations

import os
import time
import random
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from functions.logger import get_logger
from environment.variable import OS_USAGE, OS_PROFILES


class Scraper:
    logger = get_logger(__name__)

    def __init__(
        self,
        timeout: int = 30,
        max_tries_429: int = 6,
        base_backoff_s: float = 2.0,
        headers: Optional[dict] = None,
    ) -> None:
        self.timeout = timeout
        self.max_tries_429 = max_tries_429
        self.base_backoff_s = base_backoff_s

        self.session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=0,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

        if headers is None:
            self.headers = {
                "User-Agent": OS_PROFILES[OS_USAGE]["ua"],
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
            }
        else:
            self.headers = headers

    def _env_proxies(self) -> Optional[dict]:
        http_p = os.getenv("HTTP_PROXY")
        https_p = os.getenv("HTTPS_PROXY")
        if not http_p and not https_p:
            return None
        return {"http": http_p, "https": https_p}

    def fetch_html(self, url: str, referer: Optional[str] = None) -> str:
        """
        Universal HTML fetcher:
        - 429 -> backoff + retry
        - 403 -> log
        - raises for non-200
        """
        self.logger.info("Fetching URL: %s", url)

        proxies = self._env_proxies()
        if proxies:
            self.logger.warning("Using proxies from environment variables")

        headers = dict(self.headers)
        if referer is not None:
            headers["Referer"] = referer

        last_resp: Optional[requests.Response] = None

        for attempt in range(self.max_tries_429):
            resp = self.session.get(url, headers=headers, timeout=self.timeout, proxies=proxies)
            last_resp = resp

            if resp.status_code == 429:
                sleep_s = min(
                    120.0,
                    (self.base_backoff_s * (2 ** attempt)) + random.uniform(0, 1.5),
                )
                self.logger.warning(
                    "429 Too Many Requests for %s. Sleeping %.1fs (attempt %d/%d)",
                    url, sleep_s, attempt + 1, self.max_tries_429
                )
                time.sleep(sleep_s)
                continue

            if resp.status_code == 403:
                self.logger.error(
                    "403 Forbidden for %s. Likely anti-bot or network/IP blocking. "
                    "Try slower pacing, caching, different network, or cloudscraper fallback.",
                    url,
                )

            resp.raise_for_status()
            return resp.text

        if last_resp is not None:
            last_resp.raise_for_status()

        raise RuntimeError(f"Failed to fetch URL after {self.max_tries_429} tries: {url}")

    def fetch_html_with_cloudscraper(self, url: str, referer: Optional[str] = None) -> str:
        """
        Optional fallback if normal requests get 403.
        Requires: pip install cloudscraper
        """
        try:
            import cloudscraper
        except ImportError as e:
            raise RuntimeError("cloudscraper is not installed. Install via: pip install cloudscraper") from e

        self.logger.warning("Using cloudscraper fallback for: %s", url)

        proxies = self._env_proxies()
        headers = dict(self.headers)
        if referer is not None:
            headers["Referer"] = referer

        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": OS_PROFILES[OS_USAGE]["cloudscraper_platform"], "desktop": True}
        )
        resp = scraper.get(url, headers=headers, timeout=self.timeout, proxies=proxies)
        resp.raise_for_status()
        return resp.text

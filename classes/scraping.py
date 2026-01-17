### Scraper for website info ### 

# Imports
import time
import random
from typing import Optional
from curl_cffi import requests as cur_requests 

from functions.logger import get_logger
from environment.variable import OS_USAGE, OS_PROFILES

# Class: Scraping
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
        
        # --- MISSING ATTRIBUTES ADDED HERE ---
        self.last_request_time = 0.0
        self.min_delay = 3.1 
        
        if headers is None:
            # Match Chrome 122 across all fields
            self.headers = {
                "User-Agent": OS_PROFILES[OS_USAGE]["ua"], # This is Chrome/122.0.0.0
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                # MUST MATCH Chrome 122 in the User-Agent string
                "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": OS_PROFILES[OS_USAGE]["platform"],
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "TE": "trailers"
            }
        else:
            self.headers = headers

    def _env_proxies(self) -> Optional[dict]:
        import os
        http_p = os.getenv("HTTP_PROXY")
        https_p = os.getenv("HTTPS_PROXY")
        return {"http": http_p, "https": https_p} if http_p or https_p else None

    def _smart_delay(self):
        """Ensures at least 3.1s + small jitter between calls."""
        elapsed = time.time() - self.last_request_time
        # Add a tiny bit of random jitter to the base delay
        required_gap = self.min_delay + random.uniform(0, 1.0)
        
        if elapsed < required_gap:
            wait_time = required_gap - elapsed
            self.logger.info(f"Throttling for {wait_time:.2f}s to respect FBRef rules")
            time.sleep(wait_time)
        
        self.last_request_time = time.time()

    def fetch_html(self, url: str, referer: Optional[str] = None) -> str:
        # Time delay
        self._smart_delay()
        
        self.logger.info("Fetching: %s", url)
        
        headers = dict(self.headers)
        if referer:
            headers["Referer"] = referer
        # Try to scrape the data
        for attempt in range(self.max_tries_429):
            try:
                resp = cur_requests.get(
                    url, 
                    headers=headers, 
                    timeout=self.timeout, 
                    impersonate="firefox", 
                    proxies=self._env_proxies()
                )

                if resp.status_code == 429:
                    sleep_s = min(120.0, (self.base_backoff_s * (2 ** attempt)) + random.uniform(0, 1.5))
                    logger.warning("429 Too Many Requests. Sleeping %.1fs", sleep_s)
                    time.sleep(sleep_s)
                    continue

                if resp.status_code == 403:
                   logger.error("403 Forbidden. Possible IP flag or TLS mismatch.")
                
                resp.raise_for_status()
                return resp.text

            except Exception as e:
                if attempt == self.max_tries_429 - 1:
                    raise e
                ogger.warning("Attempt %d failed: %s. Retrying...", attempt + 1, str(e))
                time.sleep(self.base_backoff_s * (2 ** attempt))

        raise RuntimeError(f"Failed to fetch {url} after retries.")
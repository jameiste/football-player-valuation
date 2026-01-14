### Read data from FBREF ##

# Imports 
# fbref.py
"""
FBref-specific parsing ONLY.

Uses Scraper (universal) for fetching.
No pattern scraping.
"""

from __future__ import annotations

import re
from io import StringIO
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup, Comment

from functions.data_related import flatten_columns
from classes.scraping import Scraper

# Function: Scrape the data from fbref
def scrape_fbref(
    url: str,
    table_id: str,
    use_cloudscraper_fallback: bool = False,
) -> pd.DataFrame:
    s = Scraper()

    try:
        html = s.fetch_html(url, referer="https://fbref.com/")
    except Exception:
        if use_cloudscraper_fallback:
            html = s.fetch_html_with_cloudscraper(url, referer="https://fbref.com/")
        else:
            raise

    soup = BeautifulSoup(html, "lxml")

    # Normal DOM
    t = soup.find("table", id=table_id)
    if t is not None:
        df = pd.read_html(StringIO(str(t)), flavor="lxml")[0]
        if "Player" in df.columns:
            df = df[df["Player"] != "Player"]
        return flatten_columns(df)

    # Commented tables
    for c in soup.find_all(string=lambda x: isinstance(x, Comment)):
        c_str = str(c)
        if f'id="{table_id}"' not in c_str and f"id='{table_id}'" not in c_str:
            continue

        m = re.search(
            r"(<table[^>]*\bid=['\"]" + re.escape(table_id) + r"['\"][\s\S]*?</table>)",
            c_str,
        )
        if m:
            df = pd.read_html(StringIO(m.group(1)), flavor="lxml")[0]
            if "Player" in df.columns:
                df = df[df["Player"] != "Player"]
            return flatten_columns(df)

        df = pd.read_html(StringIO(c_str), flavor="lxml")[0]
        if "Player" in df.columns:
            df = df[df["Player"] != "Player"]
        return flatten_columns(df)

    raise ValueError(f"Table id '{table_id}' not found on page: {url}")

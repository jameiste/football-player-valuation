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
import pandas as pd
from bs4 import BeautifulSoup, Comment

# Local imports
from functions.data_related import flatten_columns
from functions.utils import find_country
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
    
    # Function: Extract the table
    def parse_table(table_html: str) -> pd.DataFrame:
        df = pd.read_html(StringIO(table_html), flavor="lxml")[0]
        # Resolve Nation problem
        if "Nation" in df.columns:
            df["Nation"] = df["Nation"].astype(str).str.split().str[-1]
        return flatten_columns(df)

    # Normal DOM
    t = soup.find("table", id=table_id)
    if t is not None:
        return parse_table(str(t))

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
            return parse_table(m.group(1))

        return parse_table(c_str)

    raise ValueError(f"Table id '{table_id}' not found on page: {url}")

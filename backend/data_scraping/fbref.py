### Read data from FBREF ##
"""
Scrape statistic of all Players out of the top 5-Leagues
fbref is the foundation of that
Put all information in dataframes
"""

# Imports 
from __future__ import annotations
import re
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup, Comment

# Local imports
from functions.data_related import flatten_columns
from functions.utils import find_country
from functions.logger import get_logger
from classes.scraping import Scraper

# Logger 
logger = get_logger(__name__)
# Function: Scrape the data from fbref
def scrape_fbref(
    url: str,
    table_id: str,
) -> pd.DataFrame:
    scraper = Scraper()

    try:
        # Use the 'impersonate' method directly as it handles the 403 and the timing
        html = scraper.fetch_html(url, referer="https://fbref.com/")
    except Exception as e:
        logger.error(f"Critical failure fetching {url}: {e}")
        raise 

    soup = BeautifulSoup(html, "lxml")
    
    # Function: Extract the table
    def parse_table(table_html: str) -> pd.DataFrame:
        df = pd.read_html(StringIO(table_html), flavor="lxml")[0]
        df = flatten_columns(df)
        # Resolve Nation problem
        if "Nation" in df.columns:
            df["Nation"] = df["Nation"].astype(str).str.split().str[-1]
        return df

    # Normal  
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

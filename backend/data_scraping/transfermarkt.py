# transfermarkt.py
"""
Transfermarkt-specific parsing ONLY.

Uses Scraper (universal) for fetching.
No pattern scraping.
"""
# Imports
from __future__ import annotations
import re
from typing import Optional
import pandas as pd
from bs4 import BeautifulSoup

# Local imports
from classes.scraping import Scraper
from functions.data_related import numeric_values_adaption 


# Function: Scrape the data from transfermarkt 
def scrape_transfermarkt(
    url: str,
    use_cloudscraper_fallback: bool = False,
) -> pd.DataFrame:
    s = Scraper()

    try:
        html = s.fetch_html(url, referer="https://www.transfermarkt.com/")
    except Exception:
        if use_cloudscraper_fallback:
            html = s.fetch_html_with_cloudscraper(url, referer="https://www.transfermarkt.com/")
        else:
            raise

    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", class_="items")
    if table is None:
        # no table -> empty page (end of pagination or blocked)
        return pd.DataFrame()

    rows = []
    for tr in table.find_all("tr", class_=lambda c: c in {"odd", "even"} if c else False):
        # Player link (usually first td.hauptlink a)
        a_player = tr.select_one("td.hauptlink a")
        player_name = a_player.get_text(strip=True) if a_player else None
        player_href = a_player.get("href") if a_player else None

        # Player ID from URL (/profil/spieler/<id>)
        player_id = None
        if player_href:
            m_id = re.search(r"/spieler/(\d+)", player_href)
            if m_id:
                player_id = m_id.group(1)

        # Club name (often as title on a centered link/img)
        a_club = tr.select_one("td.zentriert a[title]")
        club_name = a_club.get("title") if a_club else None

        # Market value (usually right aligned main link)
        mv_td = tr.select_one("td.rechts.hauptlink")
        mv_text = mv_td.get_text(strip=True) if mv_td else None
        mv_eur = numeric_values_adaption(mv_text)

        if player_name is None:
            continue

        rows.append(
            {
                "Player": player_name,
                "TM_Player_ID": player_id,
                "Club": club_name,
                "Market_Value_Text": mv_text,
                "Market_Value_EUR": mv_eur,
                "TM_URL": ("https://www.transfermarkt.com" + player_href) if player_href else None,
            }
        )

    return pd.DataFrame(rows)

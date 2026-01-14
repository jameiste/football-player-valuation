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
from urllib.parse import quote

# Local imports
from classes.scraping import Scraper
from functions.data_related import numeric_values_adaption 
from environment.variable import POSITION_MAP

# Function: Teams in the league
def teams_in_league(league: str, competition: str, season_id: int) -> pd.DataFrame:
    url = f'https://www.transfermarkt.com/{league}/startseite/wettbewerb/{competition}/saison_id/{season_id}/plus/1'
    html = Scraper().fetch_html(url, referer="https://www.transfermarkt.com/")
    soup = BeautifulSoup(html, "lxml")

    rows, seen = [], set()
    for a in soup.select('a[title][href*="/startseite/verein/"]'):
        href = a.get("href", "")
        m = re.search(r"^/([^/]+)/startseite/verein/(\d+)/saison_id/(\d+)", href)
        if not m:
            continue
        slug, club_id, sid = m.group(1), m.group(2), int(m.group(3))
        key = (club_id, sid)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"Club": a["title"], "Slug": slug, "ID": club_id})

    return pd.DataFrame(rows)
# Function: Find the ID of the team name
# def find_club_id(club_name: str) -> str:
#     url = (
#         "https://www.transfermarkt.com/"
#         f"schnellsuche/ergebnis/schnellsuche?query={quote(club_name)}"
#     )

#     html = Scraper().fetch_html(url, referer="https://www.transfermarkt.com/")
#     soup = BeautifulSoup(html, "lxml")

#     # first club result
#     a = soup.select_one('a[href*="/startseite/verein/"]')
#     if not a:
#         raise ValueError(f"Club not found: {club_name}")

#     m = re.search(r"/verein/(\d+)", a["href"])
#     if not m:
#         raise ValueError(f"Club ID not found for: {club_name}")

#     return m.group(1)

# Function: Scrape the data from transfermarkt 
def scrape_transfermarkt(
    url: str,
    club:str,
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
        
        # Nation
        nations = [img.get("title") for img in tr.select("td.zentriert img.flaggenrahmen") if img.get("title")]
        nation = nations[0] if nations else None  

        # Age
        age_td = tr.select_one("td.zentriert:not(.rueckennummer)")
        age = int(re.search(r"\((\d+)\)", age_td.get_text(strip=True))[1])
        
        # Position 
        position_td = tr.select_one("td.posrela table.inline-table tr:last-child td").get_text(strip=True)
        position = POSITION_MAP[position_td] if position_td else None
        
        
        # Market value (usually right aligned main link)
        mv_td = tr.select_one("td.rechts.hauptlink")
        mv_text = mv_td.get_text(strip=True) if mv_td else None
        mv_eur = numeric_values_adaption(mv_text)

        if player_name is None:
            continue

        rows.append(
            {
                "Player": player_name,
                "Age": age,
                "Nation": nation,
                "Position": position,
                "Player_ID": player_id,
                "Club": club,
                "Market_Value_Text": mv_text,
                "Market_Value_EUR": mv_eur,
                "TM_URL": ("https://www.transfermarkt.com" + player_href) if player_href else None,
            }
        )

    return pd.DataFrame(rows)

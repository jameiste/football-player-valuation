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
    url = f"https://www.transfermarkt.com/{league}/startseite/wettbewerb/{competition}/saison_id/{season_id}"
    html = Scraper().fetch_html(url, referer="https://www.transfermarkt.com/")
    soup = BeautifulSoup(html, "lxml")
    # Extract table 
    table = soup.select_one("div#yw2 table.items")
    rows = []

    for tr in table.select("tbody tr"):
        tds = tr.select("td")
        if len(tds) < 6:
            continue

        club_a = tds[1].select_one('a[href*="/verein/"]')
        if not club_a:
            continue

        m = re.search(r"/([^/]+)/(?:startseite|spielplan)/verein/(\d+)", club_a["href"])
        if not m:
            continue

        rows.append({
            "League_Position": int(tds[0].get_text(strip=True)),
            "Club": club_a["title"],
            "Slug": m.group(1),
            "ID": int(m.group(2)),
            "Matches": int(tds[3].get_text(strip=True)),
            "GoalDiff_%": int(tds[4].get_text(strip=True).replace("+", "")) / int(tds[3].get_text(strip=True)),
            "Points_%": int(tds[5].get_text(strip=True)) / int(tds[3].get_text(strip=True)),
        })

    return pd.DataFrame(rows)
# Function: Find the ID of the team name
# def table_with_league()

# Function: Scrape the data from transfermarkt 
def scrape_transfermarkt(
    url: str,
    club:str,
    use_cloudscraper_fallback: bool = False,
) -> pd.DataFrame:
    s = Scraper()

    try:
        # Use the 'impersonate' method directly as it handles the 403 and the timing
        html = s.fetch_html(url, referer="https://fbref.com/")
    except Exception as e:
        logger.error(f"Critical failure fetching {url}: {e}")
        raise 

    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", class_="items")
    if table is None:
        # no table -> empty page 
        return pd.DataFrame()

    rows = []
    for tr in table.find_all("tr", class_=lambda c: c in {"odd", "even"} if c else False):
        # Player link
        a_player = tr.select_one("td.hauptlink a")
        player_name = a_player.get_text(strip=True) if a_player else None
        player_href = a_player.get("href") if a_player else None

        # Player ID from URL 
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
                "Pos": position,
                "Player_ID": player_id,
                "Club": club,
                "Market_Value_Text": mv_text,
                "Market_Value_EUR": mv_eur,
                "TM_URL": ("https://www.transfermarkt.com" + player_href) if player_href else None,
            }
        )

    return pd.DataFrame(rows)

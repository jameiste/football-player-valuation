### Scrpae stats from sofascore
"""
Transfermarkt-specific parsing ONLY.

Uses Scraper (universal) for fetching.
No pattern scraping.
"""
# Imports
from __future__ import annotations
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

# Local imports
from classes.scraping import Scraper
from functions.logger import get_logger

# Logger 
logger = get_logger(__name__)

sofascore_headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# Function: Scrape stats for each player on the website
def player_stats_league_sofascore(league: str, tournament_id: int) -> pd.DataFrame:

    logger.info("Scraping league: %s", league)

    # Get current sea
    seasons_url = f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons"
    seasons = requests.get(seasons_url, headers=sofascore_headers).json()["seasons"]
    season_id = seasons[0]["id"]

    # Get teams within the league
    teams_url = f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/teams"
    teams = requests.get(teams_url, headers=sofascore_headers).json()["teams"]

    rows = []

    # Loop over teams-
    for team in teams:
        team_id = team["id"]
        team_name = team["name"]
        logger.info("Scraping stats for team: %s", team_name)

        # Team
        squad_url = f"https://api.sofascore.com/api/v1/team/{team_id}/players"
        squad = requests.get(squad_url, headers=sofascore_headers).json()["players"]

        # Loop over each player
        for info in squad:
            player = info["player"]
            if not "dateOfBirth" in player.keys():
                continue
            player_id = player["id"]
            player_name = player["name"]
            player_nation = player["country"]["alpha3"] if "alpha3" in player["country"].keys() else None
            player_age = player["dateOfBirth"]

            # Fetch website of player
            stats_url = (
                f"https://api.sofascore.com/api/v1/player/{player_id}"
                f"/unique-tournament/{tournament_id}/season/{season_id}/statistics/overall"
            )

            r = requests.get(stats_url, headers=sofascore_headers, timeout=30)
            if r.status_code != 200:
                continue

            stats_json = r.json()

            # Get the stats
            stats = stats_json.get("statistics", {}) or stats_json.get("data", {}).get("statistics", {})
            if not stats:
                continue

            # Build up the rows
            row = {
                "League": league,
                "Club": team_name,
                "Club_ID": team_id,
                "Player": player_name,
                "Player_ID": player_id,
                "Nation": player_nation,
                "Age": pd.to_datetime(player_age)
            }

            # Add all stat fields dynamically
            for stat_name, stat_value in stats.items():
                if isinstance(stat_value, (float, int)):
                    row[f"stats.{stat_name}"] = stat_value

            rows.append(row)

    return pd.DataFrame(rows)
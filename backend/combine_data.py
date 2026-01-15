### Bring all scraped data together ###

# Imports 
import pandas as pd
import re
from typing import Optional

# Local imports
from backend.data_scraping.fbref import scrape_fbref
from backend.data_scraping.transfermarkt import scrape_transfermarkt, teams_in_league
from functions.logger import get_logger
from functions.data_related import mapping_two_columns, add_date_column
from functions.utils import find_country, load_excel, store_excel, update_sheets
from environment.variable import STATS_NAME, MARKET_SHEET_NAME, SHEETS, DATA_PATH

# Logger
logger = get_logger(__name__)

# --- --- FBREF --- ---
# Leagues TODO: Should be scraped as well
fbref_leagues = {
    "Premier-League": {"id": 9, "name": "Premier-League"},
    "Bundesliga": {"id": 20, "name": "Bundesliga"},
    "La-Liga": {"id": 12, "name": "La-Liga"},
    "Serie-A": {"id": 11, "name": "Serie-A"},
    "Ligue-1": {"id": 13, "name": "Ligue-1"},
}
# Tables
fbref_tables = {
    # Core
    "stats_standard": {"page": "stats", "table_id": "stats_standard"},
    "stats_shooting": {"page": "shooting", "table_id": "stats_shooting"},
    "stats_passing": {"page": "passing", "table_id": "stats_passing"},
    "stats_passing_types": {"page": "passing_types", "table_id": "stats_passing_types"},
    "stats_gca": {"page": "gca", "table_id": "stats_gca"},
    # Defense-related
    "stats_defense": {"page": "defense", "table_id": "stats_defense"},
    "stats_possession": {"page": "possession", "table_id": "stats_possession"},
    "stats_playing_time": {"page": "playingtime", "table_id": "stats_playing_time"},
    "stats_misc": {"page": "misc", "table_id": "stats_misc"},
    # Goalkeeping
    "stats_keeper": {"page": "keepers", "table_id": "stats_keeper"},
    "stats_keeper_adv": {"page": "keepersadv", "table_id": "stats_keeper_adv"},
}

# Function: Scrape player data from fbref
def player_stats_data(update_sheets: list)->pd.DataFrame:
    # Load and initialize data
    tm_data = load_excel(name=STATS_NAME, sheet_name=MARKET_SHEET_NAME)
    overall_data = pd.DataFrame()
    update_leagues = {sheet: fbref_leagues[sheet] for sheet in update_sheets if sheet != "All"}
    # Loop through all leagues
    for league_id, league_name in update_leagues.items():
        combined_player_stats = pd.DataFrame()
        count = 0
        for table_page, table_name in fbref_tables.items():
            fbref_url = f'https://fbref.com/en/comps/{league_name["id"]}/{table_name["page"]}/{league_name["name"]}-Stats'

            data = scrape_fbref(url=fbref_url, table_id=table_name["table_id"], use_cloudscraper_fallback=True)
            data = data.drop(columns=["Rk"]) 
            # Find column that contains '90s'
            column_90 = next((c for c in data.columns if "90s" in c), None)
            if column_90 is not None:
                min_ratio_90 = data[column_90].astype(float).max() * 0.2
                data = data[data[column_90] > min_ratio_90]
            # Add League and Table name
            data["League"] = re.sub(r'[+\- ]', '_', league_name["name"])
            data["Table"] = re.sub(r'[+\- ]', '_', table_page)
            # Initialize the dataframes
            if count == 0:
                combined_player_stats = data 

            else:
              # --- define stable merge keys (only these are shared) ---
                merge_keys = ["Player", "Nation", "Pos", "Age", "Born", "Squad", "League"]
                merge_keys = [k for k in merge_keys if k in combined_player_stats.columns and k in data.columns]

                # --- columns you want ONLY ONCE in the final df (no League_x etc) ---
                single_meta_cols = {"League", "Squad", "Table", "Matches"}  # add/remove as you like

                # drop meta columns from the RIGHT df if they already exist in LEFT and are not keys
                drop_from_right = [c for c in data.columns
                                if c in combined_player_stats.columns and c in single_meta_cols and c not in merge_keys]
                data = data.drop(columns=drop_from_right, errors="ignore")

                # --- avoid collisions for non-key columns by prefixing table name ---
                feature_cols = [c for c in data.columns if c not in merge_keys]
                data = data.rename(columns={c: f"{table_page}__{c}" for c in feature_cols})

                combined_player_stats = pd.merge(
                    combined_player_stats,
                    data.copy(),
                    on=merge_keys,
                    how="outer",
                )
               
            count = count + 1
        # Map the correct entries 
        combined_player_stats = mapping_two_columns(initial_data=combined_player_stats, reference_data=tm_data, column="Player", target="Pos")  
        combined_player_stats["Date"] = add_date_column(length=combined_player_stats.shape[0])
        # Concat data          
        if overall_data.empty:
            overall_data = combined_player_stats
        else:
            overall_data = pd.concat([overall_data, combined_player_stats], ignore_index=True)
        # Store the data as excel
        store_excel(data=combined_player_stats, name=STATS_NAME, sheet_name=league_name["name"])
    if "All" in update_sheets:
        store_excel(data=overall_data, name=STATS_NAME, sheet_name="All")

    return overall_data

# --- --- Transfermarkt --- ---
# Parameters
tm_leagues = {
    "Premier-League": {"code": "GB1", "slug": "premier-league"},
    "Bundesliga": {"code": "L1", "slug": "bundesliga"},
    "La-Liga": {"code": "ES1", "slug": "laliga"},
    "Serie-A": {"code": "IT1", "slug": "serie-a"},
    "Ligue-1": {"code": "FR1", "slug": "ligue-1"},
}

# Function: Scrape the market values of the players
def market_values_data() -> pd.DataFrame:
    tm_all = pd.DataFrame()
    
    # Determine all clubs
    all_clubs = pd.DataFrame()
    for leagues in tm_leagues.keys():
        code = tm_leagues[leagues]["code"]
        clubs = teams_in_league(league=leagues.lower(),competition=code, season_id=2025)
        if all_clubs.empty:
            all_clubs = clubs
        else:
            all_clubs = pd.concat([all_clubs, clubs], ignore_index=True)
        
    # Loop through all clubs
    for i, club in all_clubs.iterrows():
        # club_id = find_club_id(club_name=club["club"])
        tm_url = f'https://www.transfermarkt.com/{club["Slug"]}/startseite/verein/{club["ID"]}'
        logger.info("Transfermarkt: %s", club["Club"])
        data = scrape_transfermarkt(url=tm_url, club=club["Club"], use_cloudscraper_fallback=True)
        
        # Add League to data
        # data["League"] = overall_data[overall_data.Squad == club].loc["League"]
        # Check if data is already loaded
        if i == 0:
            tm_all = data
        else:
            tm_all = pd.concat([tm_all, data], ignore_index=True)

    # Short form of countries
    tm_all["Nation"] = find_country(countries=tm_all.Nation, alpha=3)
    tm_all["Date"] = add_date_column(length=tm_all.shape[0])

    # --- Store ---
    store_excel(data=tm_all, name=STATS_NAME, sheet_name=MARKET_SHEET_NAME)

    return tm_all

# Function: Combine all data
def data_table():
    # Determine updates
    update_list = update_sheets()
    fbref_list = [sheet for sheet in update_list if sheet != MARKET_SHEET_NAME]
    # Run the scraping
    if MARKET_SHEET_NAME in update_list:
        market_values = market_values_data()
    if len(fbref_list) > 0:
        data_stats = player_stats_data(fbref_list)
    
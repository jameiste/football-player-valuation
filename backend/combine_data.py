### Bring all scraped data together ###

# Imports 
import pandas as pd
import re

# Local imports
from backend.data_scraping.fbref import scrape_fbref
from functions.data_related import store_parquet, store_excel
from functions.logger import get_logger
from environment.variable import STATS_NAME

# Logger
logger = get_logger(__name__)

# --- --- FBREF --- ---
# Leagues 
fbref_leagues = {
    "Premier-League": {"id": 9, "name": "Premier-League"},
    "Bundesliga": {"id": 20, "name": "Bundesliga"},
    "La-Liga": {"id": 12, "name": "La-Liga"},
    "Serie-A": {"id": 11, "name": "Serie-A"},
    "Ligue-1": {"id": 13, "name": "Ligue-1"},
}

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

# Function: 
def player_stats_data()->pd.DataFrame:
    overall_data = pd.DataFrame()
    for league_id, league_name in fbref_leagues.items():
        combined_player_stats = pd.DataFrame()
        count = 0
        for table_page, table_name in fbref_tables.items():
            fbref_url = f"https://fbref.com/en/comps/{league_name["id"]}/{table_name["page"]}/{league_name["name"]}-Stats"

            data = scrape_fbref(url=fbref_url, table_id=table_name["table_id"])
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
        if overall_data.empty:
            overall_data = combined_player_stats
        else:
            overall_data = pd.concat([overall_data, combined_player_stats], ignore_index=True)
        # Store the data as excel
        store_excel(data=combined_player_stats, name=STATS_NAME, sheet_name=league_name["name"])
    store_excel(data=overall_data, name=STATS_NAME, sheet_name="All")
    return combined_player_stats


# Function: Combine all dtaa
def data_table():
    data_stats = player_stats_data()
    return data_stats
### Bring all scraped data together ###

# Imports 
import pandas as pd
import re
from typing import Optional

# Local imports
from backend.data_scraping.fbref import scrape_fbref
from backend.data_scraping.transfermarkt import scrape_transfermarkt, teams_in_league
from backend.data_scraping.sofascore import player_stats_league_sofascore
from functions.logger import get_logger
from functions.data_related import mapping_two_columns, add_date_column, normalize_data
from functions.utils import find_country, load_excel, store_excel, update_sheets, age_by_date
from environment.variable import STATS_NAME, MARKET_SHEET_NAME, POSITION_GROUPS, NON_FEATURES

# Logger
logger = get_logger(__name__)

# --- --- Sofascore --- ---
sofascore_leagues = {
    "Premier-League": {"id": 17, "slug": "premier-league", "country": "england"},
    "Bundesliga":     {"id": 35, "slug": "bundesliga", "country": "germany"},
    "La-Liga":        {"id": 8,  "slug": "laliga", "country": "spain"},
    "Serie-A":        {"id": 23, "slug": "serie-a", "country": "italy"},
    "Ligue-1":        {"id": 34, "slug": "ligue-1", "country": "france"},
}

# Function: Run sofascore scraping
def player_stats_sofascore(update_list : list):
    tm_data = load_excel(name=STATS_NAME, sheet_name=MARKET_SHEET_NAME)
    player_stats = pd.DataFrame()
    league_list = {key: sofascore_leagues[key] for key in sofascore_leagues if key in update_list}
    for name, info in league_list.items():
        data = player_stats_league_sofascore(league=name, tournament_id=info["id"])
        # Add further info to the data
        data["Date"] = add_date_column(length=data.shape[0])
        data["Age"] = age_by_date(date = data["Age"])
        data = mapping_two_columns(initial_data=data, reference_data=tm_data, column="Player", target="Pos")
        data = mapping_two_columns(initial_data=data, reference_data=tm_data, column="Club", target="League_Position")
        data = mapping_two_columns(initial_data=data, reference_data=tm_data, column="Club", target="Goal_Diff_%")
        data = mapping_two_columns(initial_data=data, reference_data=tm_data, column="Club", target="Points_%")
        data = mapping_two_columns(initial_data=data, reference_data=tm_data, column="Club", target="Market_Value_EUR")
        data["Pos_group"] =  data["Pos"].map(POSITION_GROUPS)
        
        # Normalize data 
        data["stats.full_games"] = round(data["stats.minutesPlayed"] / 90, 2)
        features = [column for column in data.columns if column.startswith("stats.")]
        data = normalize_data(data = data, features=features, foundation_column="stats.full_games")
        # Concat the leagues
        if player_stats.empty:
            player_stats = data
        else:
            player_stats = pd.concat([player_stats, data], ignore_index=True)

        # Store the data as excel
        store_excel(data=data, name=STATS_NAME, sheet_name=name)
    if "All" in update_list:
        store_excel(data=player_stats, name=STATS_NAME, sheet_name="All")

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

            data = scrape_fbref(url=fbref_url, table_id=table_name["table_id"])
            data = data.drop(columns=["Rk"]) 

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
        # Take the top 70% in play time
        min_ratio_90 = combined_player_stats['Playing_Time.90s'].astype(float).quantile(0.3)
        combined_player_stats = combined_player_stats[combined_player_stats['Playing_Time.90s'] > min_ratio_90]
        # Map the correct entries 
        combined_player_stats = mapping_two_columns(initial_data=combined_player_stats, reference_data=tm_data, column="Player", target="Pos")
        combined_player_stats = mapping_two_columns(initial_data=combined_player_stats, reference_data=tm_data, column="Club", target="League_Position")
        combined_player_stats = mapping_two_columns(initial_data=combined_player_stats, reference_data=tm_data, column="Club", target="Goal_Diff_%")
        combined_player_stats = mapping_two_columns(initial_data=combined_player_stats, reference_data=tm_data, column="Club", target="Points_%")
        combined_player_stats = mapping_two_columns(initial_data=combined_player_stats, reference_data=tm_data, column="Club", target="Market_Value_EUR")
        combined_player_stats["Pos_group"] =  combined_player_stats["Pos"].map(POSITION_GROUPS)
        combined_player_stats["Date"] = add_date_column(length=combined_player_stats.shape[0])

        # Normalize data 
        features = [column for column in combined_player_stats.columns if column not in NON_FEATURES]
        combined_player_stats = normalize_data(data = combined_player_stats, features=features)
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
    # Mapping for multiple infos
    goal_map = dict(zip(all_clubs["Club"], all_clubs["GoalDiff_%"]))
    points_map = dict(zip(all_clubs["Club"], all_clubs["Points_%"]))
    position_map = dict(zip(all_clubs["Club"], all_clubs["League_Position"]))
    all_maps = {"Goal_Diff_%": goal_map, "Points_%": points_map, "League_Position": position_map}
    # Loop through all clubs
    for i, club in all_clubs.iterrows():
        # club_id = find_club_id(club_name=club["club"])
        tm_url = f'https://www.transfermarkt.com/{club["Slug"]}/startseite/verein/{club["ID"]}'
        logger.info("Transfermarkt: %s", club["Club"])
        data = scrape_transfermarkt(url=tm_url, club=club["Club"], use_cloudscraper_fallback=True)

        # Check if data is already loaded
        if i == 0:
            tm_all = data
        else:
            tm_all = pd.concat([tm_all, data], ignore_index=True)

    # Short form of countries
    tm_all["Nation"] = find_country(countries=tm_all.Nation, alpha=3)
    tm_all["Date"] = add_date_column(length=tm_all.shape[0])
    # Map team info
    for column, mapping in all_maps.items():
        tm_all[column] = tm_all["Club"].map(mapping)

    # --- Store ---
    store_excel(data=tm_all, name=STATS_NAME, sheet_name=MARKET_SHEET_NAME)

    return tm_all

# Function: Combine all data
def data_table():
    # Determine updates
    update_list = update_sheets(offset_date=30)
    update_list = [sheet for sheet in update_list if sheet != MARKET_SHEET_NAME]
    # Run the scraping
    if MARKET_SHEET_NAME in update_list:
        market_values = market_values_data()
    if len(update_list) > 0:
        data_stats = player_stats_sofascore(update_list=update_list)
        # data_stats = player_stats_data(fbref_list)
    
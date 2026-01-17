### Set fixed variables ###

# Import
import pandas as pd
import os
from pathlib import Path

# Local imports
from functions.system import detect_os_profile
# Paths
DATA_PATH = Path(os.getcwd(), "data")

# System data
OS_OVERRIDE =  None # Set the system manually
OS_PROFILES = {
    "linux": {
        "ua": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "cloudscraper_platform": "linux",
        "platform": "Linux",
    },
    "macos": {
        "ua": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "cloudscraper_platform": "darwin",
        "platform": "macOS",
    },
    "windows": {
        "ua": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "cloudscraper_platform": "windows",
        "platform": "Windows",
        
    },
}
OS_USAGE = detect_os_profile(OS_OVERRIDE)

# Table names
MARKET_SHEET_NAME = "Transfermarkt_Market_Values"
SHEETS = ["Premier-League", "Bundesliga", "La-Liga", "Serie-A", "Ligue-1", "All", MARKET_SHEET_NAME]
STATS_NAME = "Player_Stats"
NON_FEATURES = ["Player", "Born", "Nation", "Date", "Table", "Matches", "Squad", "Pos", "Age", "Pos_group", "League"]
POSITION_NAME = "Position_Data"
# Position based information
POSITION_MAP = {
    # Goalkeeper
    "Goalkeeper": "GK",
    # Defenders
    "Centre-Back": "CB",
    "Left-Back": "LB",
    "Right-Back": "RB",
    "Defender": "DF",
    # Wing-backs (sometimes shown separately)
    "Left Wing-Back": "LWB",
    "Right Wing-Back": "RWB",
    # Defensive / Central Midfield
    "Defensive Midfield": "DM",
    "Central Midfield": "CM",
    "Midfield": "MF",
    # Attacking Midfield
    "Attacking Midfield": "AM",
    # Wide Midfield / Wingers
    "Left Midfield": "LM",
    "Right Midfield": "RM",
    "Left Winger": "LW",
    "Right Winger": "RW",
    # Forwards
    "Centre-Forward": "ST",
    "Second Striker": "SS",
    "Forward": "FW",
}
POSITION_GROUPS = {
    "GK": "GK", "LB": "FB", "RB": "FB", "CB": "CB", 
    "DM": "DM", "CM": "CM", "RW": "AM", "LW": "AM", "AM": "AM", "LM": "AM", "RM": "AM", 
    "ST": "ST"
}
FEATURES_SCHEMA = {
    "GK": {
        "shot_stopping": [
            "stats_keeper__Performance.GA90",
            "stats_keeper__Performance.Save%",
            "stats_keeper_adv__Expected.PSxG/SoT",
            "stats_keeper_adv__Expected./90",
        ],
        "command_of_area": [
            "stats_keeper_adv__Crosses.Stp%",
            "stats_keeper_adv__Sweeper.#OPA/90",
            "stats_keeper_adv__Sweeper.AvgDist",
        ],
        "distribution": [
            "stats_keeper_adv__Passes.Att_(GK)",
            "stats_keeper_adv__Passes.Launch%",
            "stats_keeper_adv__Passes.AvgLen",
            "stats_keeper_adv__Goal_Kicks.Launch%",
        ],
        "penalties": [
            "stats_keeper__Penalty_Kicks.Save%",
        ],
    },
    "CB": {
        "defending_volume": [
            "stats_defense__Tackles.Tkl",
            "stats_defense__Int",
            "stats_defense__Clr",
            "stats_defense__Blocks.Blocks",
        ],
        "defending_quality": [
            "stats_defense__Challenges.Tkl%",
            "stats_defense__Err",
        ],
        "aerials": [
            "stats_misc__Aerial_Duels.Won",
            "stats_misc__Aerial_Duels.Won%",
        ],
        "ball_progression": [
            "stats_passing__Total.Cmp%",
            "stats_passing__Total.PrgDist",
            "stats_passing__PrgP",
            "stats_possession__Carries.PrgC",
        ],
        "discipline": [
            "stats_misc__Performance.CrdY",
            "stats_misc__Performance.CrdR",
        ],
    },
    "FB": {
        "defending": [
            "stats_defense__Tackles.Tkl",
            "stats_defense__Int",
            "stats_defense__Challenges.Tkl%",
        ],
        "progression": [
            "stats_possession__Carries.PrgC",
            "stats_possession__Carries.PrgDist",
            "stats_passing__PrgP",
        ],
        "crossing": [
            "stats_passing__CrsPA",
            "stats_passing_types__Pass_Types.Crs",
        ],
        "chance_creation": [
            "stats_passing__KP",
            "stats_passing__xAG",
        ],
        "attacking_presence": [
            "stats_possession__Touches.Att_3rd",
            "stats_possession__Touches.Att_Pen",
        ],
    },
    "DM": {
        "defensive_actions": [
            "stats_defense__Tackles.Tkl",
            "stats_defense__Int",
            "stats_defense__Tkl_Int",
        ],
        "pressing_zones": [
            "stats_defense__Tackles.Def_3rd",
            "stats_defense__Tackles.Mid_3rd",
        ],
        "passing_security": [
            "stats_passing__Total.Cmp%",
            "stats_passing__Short.Cmp%",
            "stats_passing__Medium.Cmp%",
        ],
        "progression": [
            "stats_passing__PrgP",
            "stats_possession__Carries.PrgC",
        ],
        "discipline": [
            "stats_misc__Performance.Fls",
            "stats_misc__Performance.CrdY",
        ],
    },
    "CM": {
        "ball_progression": [
            "stats_passing__PrgP",
            "stats_possession__Carries.PrgC",
            "stats_possession__Carries.PrgDist",
        ],
        "passing_range": [
            "stats_passing__Total.Cmp%",
            "stats_passing__Medium.Cmp%",
            "stats_passing__Long.Cmp%",
        ],
        "chance_creation": [
            "stats_passing__KP",
            "stats_passing__xAG",
        ],
        "defensive_support": [
            "stats_defense__Tackles.Tkl",
            "stats_defense__Int",
        ],
        "box_activity": [
            "stats_possession__Touches.Att_3rd",
            "stats_possession__Touches.Def_3rd",
        ],
    },
    "AM": {
        "chance_creation": [
            "stats_passing__KP",
            "stats_passing__xAG",
            "stats_gca__SCA.SCA90",
        ],
        "ball_carrying": [
            "stats_possession__Take_Ons.Att",
            "stats_possession__Take_Ons.Succ%",
            "stats_possession__Carries.PrgC",
        ],
        "goal_threat": [
            "Per_90_Minutes.xG",
            "Per_90_Minutes.Gls",
            "stats_shooting__Standard.Sh/90",
        ],
        "box_presence": [
            "stats_possession__Touches.Att_Pen",
        ],
        "crossing": [
            "stats_passing__CrsPA",
        ],
    },
    "ST": {
        "finishing": [
            "Per_90_Minutes.Gls",
            "Per_90_Minutes.npxG",
            "stats_shooting__Standard.G/Sh",
        ],
        "shot_volume": [
            "stats_shooting__Standard.Sh/90",
            "stats_shooting__Standard.SoT/90",
        ],
        "movement": [
            "stats_possession__Touches.Att_Pen",
            "stats_possession__Carries.PrgC",
        ],
        "link_up": [
            "stats_passing__KP",
            "stats_passing__xAG",
        ],
        "pressing": [
            "stats_defense__Tackles.Att_3rd",
        ],
    },
}

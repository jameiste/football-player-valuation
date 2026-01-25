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
NON_FEATURES = ["Player", "Nation", "Date", "Pos", "Age", "Pos_group", "League"]
POSITION_NAME = "Position_Data"
# Position based information
POSITION_MAP = {
    "Goalkeeper": "GK",
    "Centre-Back": "CB", "Left-Back": "LB", "Right-Back": "RB", "Defender": "DF",
    "Left Wing-Back": "LWB", "Right Wing-Back": "RWB",
    "Defensive Midfield": "DM", "Central Midfield": "CM", "Midfield": "MF",
    "Attacking Midfield": "AM",
    "Left Midfield": "LM", "Right Midfield": "RM", "Left Winger": "LW", "Right Winger": "RW",
    "Centre-Forward": "ST", "Second Striker": "SS", "Forward": "FW"
}

POSITION_GROUPS = {
    "GK": "GK", "LB": "FB", "RB": "FB", "CB": "CB", 
    "DM": "DM", "CM": "CM", "RW": "AM", "LW": "AM", "AM": "AM", "LM": "AM", "RM": "AM", 
    "ST": "ST"
}
FEATURES_SCHEMA_FBREF = {
    "GK": {
        "shot": ["stats_keeper__Performance.GA90", "stats_keeper__Performance.Save%", "stats_keeper_adv__Expected.PSxG/SoT", "stats_keeper_adv__Expected./90"],
        "area": ["stats_keeper_adv__Crosses.Stp%", "stats_keeper_adv__Sweeper.#OPA/90", "stats_keeper_adv__Sweeper.AvgDist"],
        "dist": ["stats_keeper_adv__Passes.Att_(GK)", "stats_keeper_adv__Passes.Launch%", "stats_keeper_adv__Passes.AvgLen", "stats_keeper_adv__Goal_Kicks.Launch%"],
        "pen": ["stats_keeper__Penalty_Kicks.Save%"]
    },
    "CB": {
        "vol": ["stats_defense__Tackles.Tkl", "stats_defense__Int", "stats_defense__Clr", "stats_defense__Blocks.Blocks"],
        "qual": ["stats_defense__Challenges.Tkl%", "stats_defense__Err"],
        "air": ["stats_misc__Aerial_Duels.Won", "stats_misc__Aerial_Duels.Won%"],
        "prog": ["stats_passing__Total.Cmp%", "stats_passing__Total.PrgDist", "stats_passing__PrgP", "stats_possession__Carries.PrgC"],
        "disc": ["stats_misc__Performance.CrdY", "stats_misc__Performance.CrdR"]
    },
    "FB": {
        "def": ["stats_defense__Tackles.Tkl", "stats_defense__Int", "stats_defense__Challenges.Tkl%"],
        "prog": ["stats_possession__Carries.PrgC", "stats_possession__Carries.PrgDist", "stats_passing__PrgP"],
        "cross": ["stats_passing__CrsPA", "stats_passing_types__Pass_Types.Crs"],
        "create": ["stats_passing__KP", "stats_passing__xAG"],
        "att": ["stats_possession__Touches.Att_3rd", "stats_possession__Touches.Att_Pen"]
    },
    "DM": {
        "def": ["stats_defense__Tackles.Tkl", "stats_defense__Int", "stats_defense__Tkl_Int"],
        "press": ["stats_defense__Tackles.Def_3rd", "stats_defense__Tackles.Mid_3rd"],
        "sec": ["stats_passing__Total.Cmp%", "stats_passing__Short.Cmp%", "stats_passing__Medium.Cmp%"],
        "prog": ["stats_passing__PrgP", "stats_possession__Carries.PrgC"],
        "disc": ["stats_misc__Performance.Fls", "stats_misc__Performance.CrdY"]
    },
    "CM": {
        "prog": ["stats_passing__PrgP", "stats_possession__Carries.PrgC", "stats_possession__Carries.PrgDist"],
        "range": ["stats_passing__Total.Cmp%", "stats_passing__Medium.Cmp%", "stats_passing__Long.Cmp%"],
        "create": ["stats_passing__KP", "stats_passing__xAG"],
        "def": ["stats_defense__Tackles.Tkl", "stats_defense__Int"],
        "box": ["stats_possession__Touches.Att_3rd", "stats_possession__Touches.Def_3rd"]
    },
    "AM": {
        "create": ["stats_passing__KP", "stats_passing__xAG", "stats_gca__SCA.SCA90"],
        "carry": ["stats_possession__Take_Ons.Att", "stats_possession__Take_Ons.Succ%", "stats_possession__Carries.PrgC"],
        "goal": ["Per_90_Minutes.xG", "Per_90_Minutes.Gls", "stats_shooting__Standard.Sh/90"],
        "box": ["stats_possession__Touches.Att_Pen"],
        "cross": ["stats_passing__CrsPA"]
    },
    "ST": {
        "finish": ["Per_90_Minutes.Gls", "Per_90_Minutes.npxG", "stats_shooting__Standard.G/Sh"],
        "vol": ["stats_shooting__Standard.Sh/90", "stats_shooting__Standard.SoT/90"],
        "move": ["stats_possession__Touches.Att_Pen", "stats_possession__Carries.PrgC"],
        "link": ["stats_passing__KP", "stats_passing__xAG"],
        "press": ["stats_defense__Tackles.Att_3rd"]
    }
}
FEATURES_SCHEMA_SOFASCORE = {
    "GK": {"shot": ["stats.saves", "stats.goalsConceded", "stats.goalsPrevented"], "area": ["stats.highClaims", "stats.punches", "stats.successfulRunsOut"], "dist": ["stats.totalPasses", "stats.accuratePassesPercentage", "stats.accurateLongBallsPercentage"], "pen": ["stats.penaltyFaced", "stats.penaltySave"]},
    "CB": {"def": ["stats.tackles", "stats.interceptions", "stats.clearances", "stats.blockedShots"], "air": ["stats.aerialDuelsWon", "stats.aerialDuelsWonPercentage"], "err": ["stats.errorLeadToShot", "stats.errorLeadToGoal"], "pass": ["stats.totalPasses", "stats.accuratePassesPercentage"]},
    "FB": {"def": ["stats.tackles", "stats.interceptions"], "cross": ["stats.totalCross", "stats.accurateCrossesPercentage"], "prog": ["stats.accurateOppositionHalfPasses"], "create": ["stats.keyPasses", "stats.expectedAssists"]},
    "DM": {"def": ["stats.tackles", "stats.tacklesWon", "stats.interceptions", "stats.ballRecovery"], "pass": ["stats.totalPasses", "stats.accuratePassesPercentage", "stats.accurateOwnHalfPasses"], "disc": ["stats.fouls", "stats.yellowCards"]},
    "CM": {"prog": ["stats.accurateOppositionHalfPasses", "stats.accurateFinalThirdPasses"], "pass": ["stats.totalPasses", "stats.accuratePassesPercentage"], "create": ["stats.keyPasses", "stats.expectedAssists"], "def": ["stats.tackles", "stats.interceptions"]},
    "AM": {"create": ["stats.keyPasses", "stats.bigChancesCreated", "stats.expectedAssists"], "drib": ["stats.successfulDribbles", "stats.successfulDribblesPercentage"], "shoot": ["stats.goals", "stats.expectedGoals", "stats.shotsOnTarget"]},
    "ST": {"finish": ["stats.goals", "stats.expectedGoals", "stats.goalConversionPercentage"], "shoot": ["stats.totalShots", "stats.shotsOnTarget"], "move": ["stats.offsides", "stats.shotsFromInsideTheBox"], "link": ["stats.keyPasses", "stats.passToAssist"]}
}

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
    },
    "macos": {
        "ua": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "cloudscraper_platform": "darwin",
    },
    "windows": {
        "ua": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "cloudscraper_platform": "windows",
    },
}
OS_USAGE = detect_os_profile(OS_OVERRIDE)

# Table names
STATS_NAME = "Player_Stats"
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

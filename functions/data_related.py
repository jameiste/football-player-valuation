### Function for data manipulation ###
# Imports
import pandas as pd
import numpy as np
import re
from datetime import datetime

# Local imports
from functions.logger import get_logger
from functions.utils import load_excel, get_best_match
from environment.variable import NON_FEATURES

# Logger
logger = get_logger(__name__)

# Function: Combin multiple indices 
def flatten_columns(data: pd.DataFrame):
    """
    Flatten MultiIndex columns using '_' as separator.
    Cleans empty and 'Unnamed' levels.
    """
    # Make column names free of irritating signs
    data = data.rename(columns=lambda x: re.sub(r'[+\- ]', '_', x))
    # Check if multi-indices even exist
    if not isinstance(data.columns, pd.MultiIndex):
        data = data
    else:
        data = data.copy()

        data.columns = [
            ".".join(
                [
                    str(level)
                    for level in col
                    if level and not str(level).startswith("Unnamed")
                ]
            )
            for col in data.columns.to_flat_index()
        ]

    # Remove columns within the frame (one column is enough)
    column = data.columns[0]
    data = data[data[column] != column]
    data = numeric_columns(data=data)

    return data

from pathlib import Path
import pandas as pd


# Function: Make numeric columns
def numeric_columns(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()

    for col in data.columns:
        # Only check object / string-like columns
        if data[col].dtype == object:
            string = data[col].dropna().astype(str)

            # Skip empty columns
            if string.empty:
                continue

            # If ALL values are numeric (digits, decimal, sign)
            if string.str.match(r'^[+-]?\d+(\.\d+)?$').all():
                data[col] = data[col].astype(float)

    return data

# Function: Adapt market values
def numeric_values_adaption(value_str: str) -> int | None:
    if value_str is None:
        return None

    s = str(value_str).strip()
    if not s or s in {"-", "—"}:
        return None

    # Normalize German formats
    s = s.replace("Mio.", "m").replace("Tsd.", "k")
    s = s.replace(".", "").replace(",", ".") 

    # Remove currency and spaces
    s = s.replace("€", "").replace(" ", "").lower()

    m = re.search(r"([0-9]+(?:\.[0-9]+)?)(m|k)?", s)
    if not m:
        return None

    num = int(m.group(1)) / 100
    unit = m.group(2)

    if unit == "m":
        return num * 1_000_000
    if unit == "k":
        return num * 1_000
    return num

# Function: From integer to million / thousands
def numeric_values_string(value: int | pd.Series) -> str | pd.Series:
    # Case 1: pandas Series
    if isinstance(value, pd.Series):
        value = value.astype(int)

        return np.where(
            value > 1_000_000,
            (value / 1_000_000).map("{:.1f}M €".format),
            (value / 1_000).map("{:.1f}k €".format)
        )

    # Case 2: single numeric value
    value = int(value)

    if value > 1_000_000:
        return f"{value / 1_000_000:.1f}M €"
    return f"{value / 1_000:.1f}k €"

# Function: Map Players to their correct position (based) on transfermarkt
def mapping_two_columns(initial_data: pd.DataFrame, reference_data: pd.DataFrame, column: str, target: str) -> pd.DataFrame:
    # Set a mapping between the two columns
    initial_data = initial_data.copy()
    unique_reference = reference_data.drop_duplicates(subset=column)
    # Get rid off weird letters
    # initial_data[column] = initial_data[column].apply(lambda x: "".join(c for c in unicodedata.normalize('NFD', x) if not unicodedata.combining(c)))
    missing = initial_data[column].unique()
    choices = unique_reference[column].tolist()

    # Create and apply mappings
    name_map = {name: get_best_match(name, choices) for name in missing}
    mapping = unique_reference.set_index(column)[target]
    initial_data[column] = initial_data[column].map(name_map)
    initial_data[target] = initial_data[column].map(mapping)


    return initial_data

# Function: Add a column that adds the scrapped date
def add_date_column(length: int) -> pd.Series:
    date = pd.Timestamp.now().normalize()
    date_series = pd.Series([date], dtype="datetime64[ns]")

    return date_series.repeat(length).reset_index(drop=True)

# Function: Normalize data by various rules
def normalize_data(data: pd.DataFrame, features: list, foundation_column: str) -> pd.DataFrame:
    # Go through all features
    excluded = (
        "percentage",
        "/90",
        "90s",
        "%",
        "rating",
        "frequency",
        "conversion",
        "appearances",
        "minute",
        "matches",
        "clean",
    )
    for i, feature in enumerate(features):
        # Check various cases
        if any(ex in feature.lower() for ex in excluded):
            continue
        else:
            data[feature] = data[feature].astype(float) / data[foundation_column]

    return data

# Function: Standardize data (fixed, select only needed columns)
def standardize_data(data: pd.DataFrame, columns_interest: list, grouping: list, column: str) -> pd.DataFrame:
    # Ensure inputs are lists
    if not isinstance(columns_interest, list):
        columns_interest = list(columns_interest)
    if not isinstance(grouping, list):
        grouping = list(grouping)

    overall_data = pd.DataFrame()

    for i, group in enumerate(grouping):
        # Select columns of interest and not more
        temp_data = data.loc[data[column].isin(group if isinstance(group, list) else [group]), NON_FEATURES + columns_interest].copy()

        # Standardize all columns
        for col in columns_interest:
            mean = temp_data[col].astype(float).mean()
            std = temp_data[col].astype(float).std()
            temp_data[col] = (temp_data[col].astype(float) - mean) / std

        # Rename columns once for this group
        temp_data.rename(columns={col: f"{column}.{col}" for col in columns_interest}, inplace=True)
        

        # Concat group data
        if i == 0:
            overall_data = temp_data
        else:
            overall_data = pd.concat([overall_data, temp_data], axis=0)

    # Reset index
    overall_data.reset_index(drop=True, inplace=True)
    return overall_data

# Function: Standarie further mutrics team and player related
def context_features(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    # exposure
    d["ctx.log_minutes"] = np.log1p(pd.to_numeric(d["stats.minutesPlayed"], errors="coerce"))
    d["ctx.full_games"]  = pd.to_numeric(d["stats.full_games"], errors="coerce")

    # age
    d["ctx.age"]  = pd.to_numeric(d["Age"], errors="coerce")
    d["ctx.age2"] = d["ctx.age"] ** 2

    # league/team strength (higher should be better)
    pos = pd.to_numeric(d["League_Position"], errors="coerce")
    d["ctx.inv_league_pos"] = 1.0 / pos.replace(0, np.nan)

    for c in ["Points_%", "Goal_Diff_%"]:
        if c in d.columns:
            d[f"ctx.{c}"] = pd.to_numeric(d[c], errors="coerce")

    return d

# Function: Make z score for other metrics
def zscore_within_group(
    df: pd.DataFrame,
    cols: list[str],
    group_cols: list[str],
    prefix: str = "LeagueCtx",
) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    g = df.groupby(group_cols, observed=True)

    for c in cols:
        s  = pd.to_numeric(df[c], errors="coerce")
        mu = g[c].transform(lambda x: pd.to_numeric(x, errors="coerce").mean())
        sd = g[c].transform(lambda x: pd.to_numeric(x, errors="coerce").std(ddof=0))

        z = (s - mu) / sd
        z = z.where(sd.notna() & (sd != 0), 0.0)

        out[f"{prefix}.{c}"] = z

    return out

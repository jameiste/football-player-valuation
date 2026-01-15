### Function for data manipulation ###
# Imports
import pandas as pd
import re
from datetime import datetime

# Local imports
from functions.logger import get_logger
from functions.utils import load_excel, get_best_match

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

# Function: Map Players to their correct position (based) on transfermarkt
def mapping_two_columns(initial_data: pd.DataFrame, reference_data: pd.DataFrame, column: str, target: str) -> pd.DataFrame:
    # Set a mapping between the two columns
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
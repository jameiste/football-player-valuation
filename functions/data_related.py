### Function for data manipulation ###
# Imports
import pandas as pd
import re

# Local imports
from functions.logger import get_logger

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
def numeric_values_adaption(value_str: str) -> float | None:
    if value_str is None:
        return None

    s = str(value_str).strip()
    if not s or s in {"-", "—"}:
        return None

    # Normalize German formats
    s = s.replace("Mio.", "m").replace("Tsd.", "k")
    s = s.replace(".", "").replace(",", ".")  # 110,00 -> 110.00

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

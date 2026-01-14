### Function for data manipulation ###
# Imports
import pandas as pd
import re
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
# Local imports
from environment.variable import DATA_PATH
from functions.logger import get_logger
# Logger
logger = get_logger(__name__)
# Function: Combin multiple indices 
def flatten_columns(df):
    """
    Flatten MultiIndex columns using '_' as separator.
    Cleans empty and 'Unnamed' levels.
    """
    # Make column names free of irritating signs
    df = df.rename(columns=lambda x: re.sub(r'[+\- ]', '_', x))
    # Check if multi-indices even exist
    if not isinstance(df.columns, pd.MultiIndex):
        data = df
    else:
        data = df.copy()

        data.columns = [
            ".".join(
                [
                    str(level)
                    for level in col
                    if level and not str(level).startswith("Unnamed")
                ]
            )
            for col in df.columns.to_flat_index()
        ]

    # Remove columns within the frame (one column is enough)
    data = data[data.Player != "Player"]
    data = numeric_columns(data=data)

    return data

from pathlib import Path
import pandas as pd

# Function: Store data as an Excel file
def store_excel(data: pd.DataFrame, name: str, sheet_name: str | None = None):
    excel_path = Path(DATA_PATH, f"{name}.xlsx")

    if sheet_name is None:
        # create / overwrite file
        data.to_excel(
            excel_path,
            index=False,
        )
    else:
        # Append or replace sheet
        mode = "a" if excel_path.exists() else "w"

        # Optional: Check if file is empty/invalid before attempting append
        if mode == "a":
            try:
                # Quick check to see if it's a valid zip
                with pd.ExcelFile(excel_path, engine="openpyxl") as f:
                    pass
            except (BadZipFile, Exception):
                mode = "w"  # Fallback to overwrite if the file is corrupted

        with pd.ExcelWriter(
            excel_path,
            engine="openpyxl",
            mode=mode,
            if_sheet_exists="replace" if mode == "a" else None,
        ) as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=False)

    append_msg = f" (append: {sheet_name})" if sheet_name else ""
    logger.info(f"DataFrame is uploaded to: {name}{append_msg}")


# Function: Store data as a parquet 
def store_parquet(data: pd.DataFrame, name: str):
    parquet_path = Path(DATA_PATH, f"{name}.parquet")
    with open(parquet_path, "wb") as f:
        table = pa.Table.from_pandas(data)
        pq.write_table(table, f)
    logger.info("DataFrame is uploaded to:", parquet_path)

# Function: Loada data from a parquet 
def load_parquet(name: str) -> pd.DataFrame:
    parquet_path = Path(DATA_PATH, f"{name}.parquet")
    table = pq.read_table(parquet_path)
    data = table.to_pandas()
    logger.info("DataFrame is loaded from:", parquet_path)

    return data


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
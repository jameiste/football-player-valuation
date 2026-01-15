### Further functions ###
# Imports
import pandas as pd
import pycountry
from typing import Literal
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from rapidfuzz import process, utils

# Local imports
from environment.variable import DATA_PATH, STATS_NAME, SHEETS
from functions.logger import get_logger

# Logger
logger = get_logger(__name__)

# Function: Look for country abbreviations
def find_country(countries: pd.Series, alpha: Literal[2, 3, "name"] = "name") -> pd.Series:
    if alpha == 2:
        param = "alpha_2"
    elif alpha == 3:
        param = "alpha_3"
    else:
        param = "name"

    def lookup(x):
        if pd.isna(x):
            return None
        try:
            c = pycountry.countries.lookup(str(x))
            return getattr(c, param)
        except LookupError:
            return None

    return countries.apply(lookup)

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

# Function: Load excel / sheet
def load_excel(name: str, sheet_name: str | None = None) -> pd.DataFrame:
    excel_path = Path(DATA_PATH, f"{name}.xlsx")

    try:
        return pd.read_excel(
            excel_path,
            sheet_name=sheet_name,
            engine="openpyxl",
        )
    except FileNotFoundError:
        logger.error(f"Excel file not found: {name}.xlsx")
        raise


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


# Function: Check if an update is necessary
def date_update_check(date: pd.Timestamp, offset_days: int = 30) -> bool:
    current_date = pd.Timestamp.now().normalize()
    offset_date = current_date - timedelta(days=offset_days)
    if date < offset_date:
        return True
    else:
        return False

# Function: Determine which sheets needs to be updated:
def update_sheets() -> list:
    # Check if the data already exists
    data_path = Path(DATA_PATH, f"{STATS_NAME}.xlsx")
    update_sheets = SHEETS.copy()
    if data_path.exists():
        # Check if all Sheets are present
        data  = load_excel(name=STATS_NAME)
        existing = list(data.keys())
        # prese = [sheet for sheet in SHEETS if sheet in existing]
        
        # Check from when the update is
        for sheet in existing:
            temp_data = pd.DataFrame(data.get(sheet))
            if "Date" in temp_data.columns:
                temp_date = temp_data.Date.iloc[0]
            else:
                continue

            if not date_update_check(date=temp_date, offset_days=30):
                update_sheets.remove(sheet)
    if len(update_sheets) > 0:
        logger.info("Following sheets need to be updated:", update_sheets)
    else:
        logger.info("All sheets are up to date")
    return update_sheets

# Function: Find the closest name
def get_best_match(name: str, choices: list) -> str | None:
    # Find the best match with a similarity score
    match = process.extractOne(name, choices, processor=utils.default_process)
    # Only return if the match is very likely (score > 70/100)
    if match and match[1] > 70:
        return match[0]
    return None


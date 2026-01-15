### File to generate over all scores for each player ###
"""
Compare players across their:
    - position
    - league
    - age group
Make an overall score how well they perform and give a final score (like Fifa)
Besides that we want maybe sub-scores in various categories but that is secondary

Furthermore based on soft and hard factors a transfer market value is approximated and compared to its real value
"""
# Imports
import pandas as pd

# Local imports
from functions.utils import load_excel, store_excel, update_sheets
from functions.data_related import standardize_data
from environment.variable import STATS_NAME, MARKET_SHEET_NAME, SHEETS, POSITION_NAME, FEATURES_SCHEMA, NON_FEATURES, POSITION_GROUPS
# Function: Build up the scoring
def prepare_scoring():
    # Data
    tm_data = load_excel(name=STATS_NAME, sheet_name=MARKET_SHEET_NAME)
    stats_data = load_excel(name=STATS_NAME, sheet_name="All")
    # Group mapping
    group_dict = {
        "League": stats_data.League.unique().tolist(),
        "Age": [range(0,19), range(19,23), range(23, 30), range(30,101)],
        "Pos_group": stats_data.Pos_group.unique().tolist() 
    }
    pos_groups = set(POSITION_GROUPS.values())
    # Standardize the data
    all_data = pd.DataFrame()
    overall_data = pd.DataFrame()
    for position_group, features in FEATURES_SCHEMA.items():
        # Filter data for the current position
        temp_pos_data = stats_data[stats_data["Pos_group"] == position_group]
        features = list(f for v in features.values() for f in v)
        feature_data = pd.DataFrame()
        # Loop through the different categories
        for column, group in group_dict.items():
            data = standardize_data(data=temp_pos_data, columns_interest=features, grouping=group, column=column)
            if feature_data.empty:
                feature_data = data
            else:
               feature_data = pd.merge(feature_data, data, on=NON_FEATURES)

        # Concat data
        if overall_data.empty:
            overall_data = feature_data
        else:
           overall_data = pd.concat([overall_data, feature_data])
        # Store 
        store_excel(data=feature_data, name=POSITION_NAME, sheet_name=position_group)

    # Store 
    store_excel(data=overall_data, name=POSITION_NAME, sheet_name="All")






# Function: Run the model prediction
def run_scoring():
    if update_sheets(offset_date=30):
        prepare_scoring()
    return None
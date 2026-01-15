### Runner for the app ###
# Imports
import os
# Local imports
from backend.combine_data import data_table
from backend.metric_analyzation.scoring import run_scoring
from environment.variable import DATA_PATH

# Make the data directory if not existing
if not DATA_PATH.exists():
    os.mkdir(DATA_PATH)
    
# Just run what is present now
data = data_table()
score = run_scoring()
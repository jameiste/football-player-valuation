# Runner for the app

# Local imports
from backend.combine_data import data_table
from backend.metric_analyzation.scoring import run_scoring

# Just run what is present now
data = data_table()
score = run_scoring()
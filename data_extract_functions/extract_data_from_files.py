import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(SCRIPT_DIR, "..", "data", "team_venue_data.json")

with open(DATA_PATH, "r") as f:
    team_venue_data = json.load(f)

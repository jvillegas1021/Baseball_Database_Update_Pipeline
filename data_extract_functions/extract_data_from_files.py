import json
import os

with open(os.path.join("data", "team_venue_data.json"), "r") as f:
    team_venue_data = json.load(f)

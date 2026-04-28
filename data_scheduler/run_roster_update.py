from pipelines.daily_roster_update import run_daily_roster_update
from pipelines.daily_pitcher_recent_form_update import run_starting_pitchers_recent_form_update

def roster_update ():
  
  run_daily_roster_update()
  run_starting_pitchers_recent_form_update()

if __name__ == "__main__":
    roster_update()

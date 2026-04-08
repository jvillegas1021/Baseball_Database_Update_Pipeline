import pandas as pd
from datetime import datetime
import pytz
import numpy as np
from functools import reduce

from data_extract_functions.extract_player_data import batter_splits

from data_transform_functions.utility_functions import filter_relievers, safe_div, convert_ip


def process_pitcher_df(pitcher_df: pd.DataFrame) -> pd.DataFrame:
    pitcher_df['update date'] = datetime.now(pytz.timezone("America/New_York"))
    return(pitcher_df)

def process_batter_df(batter_df: pd.DataFrame) -> pd.DataFrame:
    batter_df['update date'] = datetime.now(pytz.timezone("America/New_York"))
    return(batter_df)



def process_team_pitching_df(
        game_id: int,
        game_official_date,
        team_name: str,
        team_id: int,
        team_pitchers_player_ids: list[int],
        all_pitcher_stats_statsapi: pd.DataFrame,
        all_pitcher_stats_statcast: pd.DataFrame
    ) -> pd.DataFrame:

    # --- Merge full pitcher dataset ---
    all_pitcher_stats_combined = pd.merge(
        all_pitcher_stats_statsapi,
        all_pitcher_stats_statcast,
        how='left',
        on=['xMLBAMID', 'season']
    )

    # --- Filter to pitchers on this team ---
    roster_pitching_df = all_pitcher_stats_combined[
        all_pitcher_stats_combined['xMLBAMID'].isin(team_pitchers_player_ids)
    ].copy()

    if roster_pitching_df.empty:
        return None

    # --- Sum all numeric stats ---
    team = roster_pitching_df.sum(numeric_only=True)

    # --- Constants ---
    league_xwoba = 0.3162979120429958
    league_era = 4.15
    fip_constant = 3.1495185210234546

    # --- Derived totals ---
    team["IP"] = team["outs"] / 3
    team["BF"] = (
        team["strikeOuts"] +
        team["baseOnBalls"] +
        team["hitByPitch"] +
        team["batted_balls"]
    )

    # --- Run prevention ---
    team["ERA"] = (team["earnedRuns"] / team["IP"]) * 9
    team["WHIP"] = (team["baseOnBalls"] + team["hits"]) / team["IP"]
    team["BABIP"] = (team["hits"] - team["homeRuns"]) / (
        team["batted_balls"] - team["homeRuns"]
    )
    team["RS/9"] = (team["runs"] / team["IP"]) * 9
    team["HR/9"] = (team["homeRuns"] / team["IP"]) * 9

    # --- K/BB family ---
    team["K%"] = team["strikeOuts"] / team["BF"]
    team["BB%"] = team["baseOnBalls"] / team["BF"]
    team["K-BB%"] = team["K%"] - team["BB%"]
    team["K/BB"] = team["strikeOuts"] / team["baseOnBalls"]
    team["K/9"] = (team["strikeOuts"] / team["IP"]) * 9

    # --- Contact quality ---
    team["EV"] = team["launch_speed_sum"] / team["batted_balls"]
    team["LA"] = team["launch_angle_sum"] / team["batted_balls"]
    team["Hard%"] = team["hard_hit_balls"] / team["batted_balls"]
    team["Barrel%"] = team["barrel_balls"] / team["batted_balls"]

    team["GB%"] = team["ground_balls"] / team["batted_balls"]
    team["FB%"] = team["fly_balls"] / team["batted_balls"]
    team["LD%"] = team["line_drives"] / team["batted_balls"]
    team["HR/FB"] = team["homeRuns"] / team["fly_balls"]

    team["DP%"] = team["groundIntoDoublePlay"] / (
        team["BF"] - team["strikeOuts"] - team["baseOnBalls"] - team["hitByPitch"]
    )

    team["GO/AO"] = team["groundOuts"] / team["airOuts"]

    # --- Plate discipline ---
    team["Zone%"] = team["pitches_in_zone"] / team["pitches"]
    team["Z-Swing%"] = team["swings_in_zone"] / team["pitches_in_zone"]
    team["O-Swing%"] = team["swings_out_zone"] / (team["pitches"] - team["pitches_in_zone"])

    team["Contact%"] = team["contacted_balls"] / team["swings"]
    team["Z-Contact%"] = team["contacted_balls_in_zone"] / team["swings_in_zone"]
    team["O-Contact%"] = team["contacted_balls_out_zone"] / team["swings_out_zone"]

    team["SwStr%"] = team["whiffs"] / team["pitches"]
    team["C+SwStr%"] = (team["called_strikes"] + team["whiffs"]) / team["pitches"]
    team["F-Strike%"] = team["first_pitch_strikes"] / team["first_pitches"]

    # --- xERA ---
    team_xwoba = team["xWOBA_allowed"] / team["batted_balls"]
    team["xERA"] = league_era + (team_xwoba - league_xwoba) * 1.15 * 9

    # --- FIP ---
    team["FIP"] = (
        (13 * team["homeRuns"] +
         3 * (team["baseOnBalls"] + team["hitByPitch"]) -
         2 * team["strikeOuts"]) / team["IP"]
    ) + fip_constant

    # --- Build final DataFrame ---
    identifiers = {
        "gamePk": game_id,
        "officialDate": game_official_date,
        "team_name": team_name,
        "team_id": team_id
    }

    team_df = pd.DataFrame([{**identifiers, **team.to_dict()}])
    team_df["update date"] = datetime.now(pytz.timezone("America/New_York"))

    return team_df



def process_team_batting_df(game_id: int, game_official_date, team_name: str, team_id: int, team_batters_player_ids: list[int],
                            all_batter_stats_statsapi: pd.DataFrame, all_batter_stats_statcast: pd.DataFrame) -> pd.DataFrame:
    wBB = 0.691
    wHBP = 0.722
    w1B = 0.882
    w2B = 1.252
    w3B = 1.584
    wHR = 2.037
    
    league_woba = 0.313
    wOBAScale = 1.232
    R_PA_lg = 0.118

    # merge dfs

    all_batter_stats_combined = pd.merge(
        all_batter_stats_statsapi,
        all_batter_stats_statcast,
        how='left',
        on=['xMLBAMID', 'season']
    )
    
    roster_batting_df = all_batter_stats_combined[
        all_batter_stats_combined['xMLBAMID'].isin(team_batters_player_ids)
        ].copy()

    
    if roster_batting_df.empty or roster_batting_df.empty:
        return None

    roster_batting_df['singles'] = roster_batting_df['hits'] - roster_batting_df['homeRuns'] - roster_batting_df['triples'] - roster_batting_df['doubles']
    
    # Counting stats projected to 162 games (talent-ish, per-game played)
    count_stats = [
    'strikeOuts', 'hits', 'groundIntoDoublePlay', 'singles', 'doubles', 'triples', 'homeRuns', 'baseOnBalls', 'sacFlies', 'hitByPitch', 'GB', 'FB', 'LD', 'atBats', 'plateAppearances'
    ]
    
    team_counting_stats_results = {}
    
    # Player-level games played
    g = roster_batting_df['gamesPlayed']
    
    for stat in count_stats:
        stat_values = roster_batting_df[stat]
    
        # Per-game rate per player
        stat_per_g = stat_values / g
    
        # Project each player to 162 games
        stat_162_player = stat_per_g * 162

        # Team projection = sum of player projections
        team_counting_stats_results[stat] = stat_162_player.sum()


    h     = team_counting_stats_results['hits']
    ab    = team_counting_stats_results['atBats']
    bb    = team_counting_stats_results['baseOnBalls']
    sf    = team_counting_stats_results['sacFlies']
    hbp   = team_counting_stats_results['hitByPitch']
    gb    = team_counting_stats_results['GB']
    fb    = team_counting_stats_results['FB']
    ld    = team_counting_stats_results['LD']
    pa    = team_counting_stats_results['plateAppearances']
    one_b = team_counting_stats_results['singles']
    two_b = team_counting_stats_results['doubles']
    three_b = team_counting_stats_results['triples']
    hr    = team_counting_stats_results['homeRuns']
    so    = team_counting_stats_results['strikeOuts']
    gdp   = team_counting_stats_results['groundIntoDoublePlay']


    k_perc = so / pa if pa > 0 else 0
    bb_perc = bb / pa if pa > 0 else 0

    gb_fb_ld = gb + fb + ld
    gb_perc = gb / gb_fb_ld if gb_fb_ld > 0 else 0
    fb_perc = fb / gb_fb_ld if gb_fb_ld > 0 else 0
    ld_perc = ld / gb_fb_ld if gb_fb_ld > 0 else 0

    avg = h / ab if ab > 0 else 0
    tb = (one_b + (2 * two_b) + (3 * three_b) + (4 * hr))
    slg = tb / ab if ab > 0 else 0
    iso = slg - avg
    babip = (h - hr) / (ab - so - hr + sf) if (ab - so - hr + sf) > 0 else 0
    obp = (h + bb + hbp) / (ab + bb + hbp + sf) if (ab + bb + hbp + sf) > 0 else 0
    bb_k = bb / so if so > 0 else 0
    hr_fb = hr / fb if fb > 0 else 0

    team_hard      = safe_div(roster_batting_df["hard_hit_balls"].sum(),
                          roster_batting_df["batted_balls"].sum())

    team_z_swing   = safe_div(roster_batting_df["swings_in_zone"].sum(),
                              roster_batting_df["pitches_in_zone"].sum())
    
    team_z_contact = safe_div(roster_batting_df["contacted_balls_in_zone"].sum(),
                              roster_batting_df["swings_in_zone"].sum())
    
    team_contact   = safe_div(roster_batting_df["contacted_balls"].sum(),
                              roster_batting_df["swings"].sum())
    
    team_o_contact = safe_div(roster_batting_df["contacted_balls_out_zone"].sum(),
                              roster_batting_df["swings_out_zone"].sum())
    
    team_o_swing   = safe_div(roster_batting_df["swings_out_zone"].sum(),
                              roster_batting_df["pitches_out_zone"].sum())


    # --- Team wOBA ---
    team_woba_numerator = (
        wBB  * bb +
        wHBP * hbp +
        w1B  * one_b +
        w2B  * two_b +
        w3B  * three_b +
        wHR  * hr
    )
    
    team_woba = team_woba_numerator / pa if pa > 0 else 0
    
    
    # --- Team wRAA ---
    team_wraa = ((team_woba - league_woba) / wOBAScale) * pa if pa > 0 else 0
    
    
    # --- Team wRC ---
    team_wrc = team_wraa + (R_PA_lg * pa)
    
    
    # --- Team wRC+ ---
    team_wrc_plus = 100 * ((team_wrc / pa) / R_PA_lg) if pa > 0 else 0


    team_df = pd.DataFrame({
        'gamePk': [game_id],
        'officialDate': [game_official_date],
        'team_name': [team_name],
        'team_id': [team_id],
        "K%": [k_perc],
        "GB%": [gb_perc],
        "FB%": [fb_perc],
        "LD%": [ld_perc],
        "BB%": [bb_perc],
        "ISO": [iso],
        "BABIP": [babip],
        "AVG": [avg],
        "OBP": [obp],
        "SLG": [slg],
        "BB/K": [bb_k],
        "HR/FB": [hr_fb],
        "Hard%": [team_hard],
        "O-Swing%": [team_o_swing],
        "Z-Swing%": [team_z_swing],
        "Z-Contact%": [team_z_contact],
        "Contact%": [team_contact],
        "O-Contact%": [team_o_contact],
        'SO': [team_counting_stats_results['strikeOuts']],
        'GDP': [team_counting_stats_results['groundIntoDoublePlay']],
        '1B': [team_counting_stats_results['singles']],
        '2B': [team_counting_stats_results['doubles']],
        '3B': [team_counting_stats_results['triples']],
        'HR': [team_counting_stats_results['homeRuns']],
        'BB': [team_counting_stats_results['baseOnBalls']],
        'SF': [team_counting_stats_results['sacFlies']],
        'HBP': [team_counting_stats_results['hitByPitch']],
        'H': [team_counting_stats_results['hits']],
        'GB': [team_counting_stats_results['GB']],
        'FB': [team_counting_stats_results['FB']],
        'LD': [team_counting_stats_results['LD']],
        "wOBA": [team_woba],
        "wRAA": [team_wraa],
        "wRC": [team_wrc],
        "wRC+": [team_wrc_plus]
    })
    team_df['hitter_player_ids'] = [team_batters_player_ids]
    team_df['update date'] = datetime.now(pytz.timezone("America/New_York"))

    return team_df

def process_batter_splits():
    
    data_list_rhp = []
    data_list_lhp = []
    for i in range(1,4):
        data_rhp = batter_splits(2, i)
        data_list_rhp.append(data_rhp)
        data_lhp = batter_splits(1, i)
        data_list_lhp.append(data_lhp)
    
    merge_keys = ['Season', 'playerName', 'playerId']
    
    hitters_rhps = reduce(
        lambda left, right: left.merge(right, on=merge_keys, how='outer'),
        data_list_rhp
    )
    hitters_lhps = reduce(
        lambda left, right: left.merge(right, on=merge_keys, how='outer'),
        data_list_lhp
    )
    hitters_both = hitters_rhps.merge(
        hitters_lhps,
        on=merge_keys,
        how='inner',
        suffixes=("_vs_rhp", "_vs_lhp")
    )

    hitters_both['wOBA_splits'] = hitters_both['wOBA_vs_lhp'] - hitters_both['wOBA_vs_rhp']
    hitters_both['ISO_splits'] = hitters_both['ISO_vs_lhp'] - hitters_both['ISO_vs_rhp']
    hitters_both['BB%_splits'] = hitters_both['BB%_vs_lhp'] - hitters_both['BB%_vs_rhp']
    hitters_both['K%_splits'] = hitters_both['K%_vs_lhp'] - hitters_both['K%_vs_rhp']

    hitters_both['update date'] = datetime.now(pytz.timezone("America/New_York"))

  
    batter_splits_df = hitters_both

    return batter_splits_df

def compute_count_stats_pitcher(statcast_df) :
    
    pitcher_ids = statcast_df['pitcher'].unique()
    
    pitcher_df = pd.DataFrame(index=pitcher_ids)
    
    pitcher_df["xMLBAMID"] = pitcher_df.index

    throws_lookup = (
    statcast_df.groupby("pitcher")["p_throws"]
    .first()                     # or .unique().str[0]
    .rename("Throws")
    )

    pitcher_df["Throws"] = pitcher_df["xMLBAMID"].map(throws_lookup)

    season = statcast_df['game_year'].unique()
    
    pitcher_df['season'] = season[0]
    
    strike_zone = [1,2,3,4,5,6,7,8,9]
    
    
    swinging_strike_event_list = ['swinging_strike', 'swinging_strike_blocked']
    
    contact_event_list = ['foul', 'foul_tip', 'hit_into_play', 'foul_pitchout']
    
    strike_event_list = [
        'foul', 'foul_tip', 'hit_into_play', 'foul_pitchout',
        'swinging_strike', 'swinging_strike_blocked', 'called_strike'
    ]
    
    swing_event_list = contact_event_list + swinging_strike_event_list
    
    strike_out_event_list = ['strikeout', 'strikeout_double_play']
    
    walk_event_list = ['walk', 'intent_walk']
    
    out_event_list = [
        'grounded_into_double_play',
        'field_out',
        'force_out',
        'fielders_choice_out',
        'double_play',
        'triple_play',
        'sac_fly',
        'sac_fly_double_play',
        'strikeout',
        'strikeout_double_play'
    ]
    
    hit_event_list = [
        'single',
        'double',
        'triple',
        'home_run'
    ]
    
    ###### Calculate Raw Counts ######
    
    pitches = (statcast_df
                     .groupby('pitcher')
                     .size()
                    )

    games_played = (
    statcast_df
    .groupby(['pitcher', 'game_pk'])
    .size()
    .reset_index()
    .groupby('pitcher')['game_pk']
    .nunique()
    )
    
    pitches_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone)]
                              .groupby('pitcher')
                              .size()
                             )
    
    pitches_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone)]
                              .groupby('pitcher')
                              .size()
                             )
    
    
    swings = (statcast_df[statcast_df['description'].isin(swing_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    swings_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swing_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    swings_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swing_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    contacted_balls = (statcast_df[statcast_df['description'].isin(contact_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    contacted_balls_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(contact_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    contacted_balls_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(contact_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    whiffed_balls = (statcast_df[statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    whiffed_balls_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    whiffed_balls_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    called_strikes = (statcast_df[statcast_df['description'] == 'called_strike']
                              .groupby('pitcher')
                              .size()
                             )
    
    first_pitches = (
        statcast_df[statcast_df['pitch_number'] == 1]
        .groupby('pitcher')
        .size()
    )
    
    first_pitch_strikes = (
        statcast_df[
            (statcast_df['pitch_number'] == 1) &
            (statcast_df['description'].isin(strike_event_list))
        ]
        .groupby('pitcher')
        .size()
    )
    
    strike_outs = (statcast_df[statcast_df['events'].isin(strike_out_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    walks = (statcast_df[statcast_df['events'].isin(walk_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    hit_by_pitch = (statcast_df[statcast_df['events'] == 'hit_by_pitch']
                              .groupby('pitcher')
                              .size()
                             )
    
    outs = (statcast_df[statcast_df['events'].isin(out_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    hits = (statcast_df[statcast_df['events'].isin(hit_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    homeruns = (statcast_df[statcast_df['events'] == 'home_run']
                              .groupby('pitcher')
                              .size()
                             )
    
    ip = outs / 3.0
    
    ########### batted ball section ######################
    
    pitcher_df_filtered_contact_only = statcast_df[statcast_df['bb_type'].notna()].copy()
    
    line_drives = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'line_drive']
                   .groupby('pitcher')
                   .size()
                  )
    
    ground_balls = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'ground_ball']
                   .groupby('pitcher')
                   .size()
                  )
    
    fly_balls = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'fly_ball']
                   .groupby('pitcher')
                   .size()
                  )
    popups = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'popup']
                   .groupby('pitcher')
                   .size()
                  )
    
    batted_balls = (
        pitcher_df_filtered_contact_only
            .groupby('pitcher')
            .size()
    )
    
    hard_hit_balls = (
        pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['launch_speed'] >= 95]
            .groupby('pitcher')
            .size()
    )
    
    exit_velo = pitcher_df_filtered_contact_only['launch_speed']
    launch_angle = pitcher_df_filtered_contact_only['launch_angle']
    
    min_launch_angle = 26 - (exit_velo - 98)
    max_launch_angle = 30 + (exit_velo - 98)
    
    min_launch_angle = min_launch_angle.clip(lower=8)
    max_launch_angle = max_launch_angle.clip(upper=50)
    
    pitcher_df_filtered_contact_only['barrel'] = (
        (exit_velo >= 98) &
        (launch_angle >= min_launch_angle) &
        (launch_angle <= max_launch_angle)
    )
    
    barrel_balls = (
        pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['barrel']]
            .groupby('pitcher')
            .size()
    )
    
    launch_speed_sum = (
        pitcher_df_filtered_contact_only
            .groupby('pitcher')['launch_speed']
            .sum()
    )
    
    launch_angle_sum = (
        pitcher_df_filtered_contact_only
            .groupby('pitcher')['launch_angle']
            .sum()
    )
    
    xwoba_allowed = (
        statcast_df
        .groupby("pitcher")["estimated_woba_using_speedangle"]
        .sum()
    )
    
    league_xwoba = 0.3162979120429958
    
    league_era = 4.15
    
    season = statcast_df['game_year'].unique()
    
    
    ########################### add counts ###########################
    pitcher_df['games_played'] = games_played
    
    pitcher_df['IP'] = ip
    pitcher_df["pitches"] = pitches
    pitcher_df["pitches_in_zone"] = pitches_in_strike_zone
    pitcher_df["pitches_out_zone"] = pitches_outside_strike_zone
    pitcher_df["swings"] = swings
    pitcher_df["swings_in_zone"] = swings_in_strike_zone
    pitcher_df["swings_out_zone"] = swings_outside_strike_zone
    
    pitcher_df["contacted_balls"] = contacted_balls
    pitcher_df["contacted_balls_in_zone"] = contacted_balls_in_strike_zone
    pitcher_df["contacted_balls_out_zone"] = contacted_balls_outside_strike_zone
    
    pitcher_df["whiffs"] = whiffed_balls
    pitcher_df["whiffs_in_zone"] = whiffed_balls_in_strike_zone
    pitcher_df["whiffs_out_zone"] = whiffed_balls_outside_strike_zone
    
    pitcher_df["called_strikes"] = called_strikes
    
    pitcher_df["first_pitches"] = first_pitches
    pitcher_df["first_pitch_strikes"] = first_pitch_strikes
    
    pitcher_df["strikeouts"] = strike_outs
    pitcher_df["walks"] = walks
    pitcher_df["hit_by_pitch"] = hit_by_pitch
    pitcher_df["outs"] = outs
    pitcher_df["hits"] = hits
    pitcher_df["home_runs"] = homeruns
    
    pitcher_df["ground_balls"] = ground_balls
    pitcher_df["fly_balls"] = fly_balls
    pitcher_df["line_drives"] = line_drives
    pitcher_df["popups"] = popups
    
    pitcher_df["batted_balls"] = batted_balls
    pitcher_df["hard_hit_balls"] = hard_hit_balls
    pitcher_df["barrel_balls"] = barrel_balls
    pitcher_df["launch_speed_sum"] = launch_speed_sum
    pitcher_df["launch_angle_sum"] = launch_angle_sum
    
    pitcher_df['xWOBA_allowed'] = xwoba_allowed

    return(pitcher_df)

def compute_count_stats_pitcher_statcast(statcast_df) :
    
    pitcher_ids = statcast_df['pitcher'].unique()
    
    pitcher_df = pd.DataFrame(index=pitcher_ids)
    
    pitcher_df["xMLBAMID"] = pitcher_df.index

    throws_lookup = (
    statcast_df.groupby("pitcher")["p_throws"]
    .first()                     # or .unique().str[0]
    .rename("Throws")
    )

    pitcher_df["Throws"] = pitcher_df["xMLBAMID"].map(throws_lookup)

    season = statcast_df['game_year'].unique()
    
    pitcher_df['season'] = season[0]
    
    strike_zone = [1,2,3,4,5,6,7,8,9]
    
    swinging_strike_event_list = ['swinging_strike', 'swinging_strike_blocked']
    
    contact_event_list = ['foul', 'foul_tip', 'hit_into_play', 'foul_pitchout']
    
    strike_event_list = [
        'foul', 'foul_tip', 'hit_into_play', 'foul_pitchout',
        'swinging_strike', 'swinging_strike_blocked', 'called_strike'
    ]
    
    swing_event_list = contact_event_list + swinging_strike_event_list
    
    strike_out_event_list = ['strikeout', 'strikeout_double_play']
    
    walk_event_list = ['walk', 'intent_walk']
    
    out_event_list = [
        'grounded_into_double_play',
        'field_out',
        'force_out',
        'fielders_choice_out',
        'double_play',
        'triple_play',
        'sac_fly',
        'sac_fly_double_play',
        'strikeout',
        'strikeout_double_play'
    ]
    
    hit_event_list = [
        'single',
        'double',
        'triple',
        'home_run'
    ]

    
    ###### Calculate Raw Counts ######
    
    pitches = (statcast_df
                     .groupby('pitcher')
                     .size()
                    )

    games_played = (
    statcast_df
    .groupby(['pitcher', 'game_pk'])
    .size()
    .reset_index()
    .groupby('pitcher')['game_pk']
    .nunique()
    )
    
    pitches_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone)]
                              .groupby('pitcher')
                              .size()
                             )
    
    pitches_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone)]
                              .groupby('pitcher')
                              .size()
                             )

    
    swings = (statcast_df[statcast_df['description'].isin(swing_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    swings_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swing_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    swings_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swing_event_list)]
                              .groupby('pitcher')
                              .size()
                             )
    
    contacted_balls = (statcast_df[statcast_df['description'].isin(contact_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    contacted_balls_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(contact_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    contacted_balls_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(contact_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    whiffed_balls = (statcast_df[statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    whiffed_balls_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    whiffed_balls_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('pitcher')
                     .size()
                    )
    
    called_strikes = (statcast_df[statcast_df['description'] == 'called_strike']
                              .groupby('pitcher')
                              .size()
                             )
    
    first_pitches = (
        statcast_df[statcast_df['pitch_number'] == 1]
        .groupby('pitcher')
        .size()
    )
    
    first_pitch_strikes = (
        statcast_df[
            (statcast_df['pitch_number'] == 1) &
            (statcast_df['description'].isin(strike_event_list))
        ]
        .groupby('pitcher')
        .size()
    )
    
    
    ########### batted ball section ######################
    
    pitcher_df_filtered_contact_only = statcast_df[statcast_df['bb_type'].notna()].copy()
    
    line_drives = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'line_drive']
                   .groupby('pitcher')
                   .size()
                  )
    
    ground_balls = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'ground_ball']
                   .groupby('pitcher')
                   .size()
                  )
    
    fly_balls = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'fly_ball']
                   .groupby('pitcher')
                   .size()
                  )
    popups = (pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['bb_type'] == 'popup']
                   .groupby('pitcher')
                   .size()
                  )
    
    batted_balls = (
        pitcher_df_filtered_contact_only
            .groupby('pitcher')
            .size()
    )
    
    hard_hit_balls = (
        pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['launch_speed'] >= 95]
            .groupby('pitcher')
            .size()
    )

    med_hit_balls = (
        pitcher_df_filtered_contact_only[(pitcher_df_filtered_contact_only['launch_speed'] >= 80) & (pitcher_df_filtered_contact_only['launch_speed'] < 95)]
            .groupby('pitcher')
            .size()
    )

    soft_hit_balls = (
        pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['launch_speed'] < 80]
            .groupby('pitcher')
            .size()
    )

    
    exit_velo = pitcher_df_filtered_contact_only['launch_speed']
    launch_angle = pitcher_df_filtered_contact_only['launch_angle']
    
    min_launch_angle = 26 - (exit_velo - 98)
    max_launch_angle = 30 + (exit_velo - 98)
    
    min_launch_angle = min_launch_angle.clip(lower=8)
    max_launch_angle = max_launch_angle.clip(upper=50)
    
    pitcher_df_filtered_contact_only['barrel'] = (
        (exit_velo >= 98) &
        (launch_angle >= min_launch_angle) &
        (launch_angle <= max_launch_angle)
    )
    
    barrel_balls = (
        pitcher_df_filtered_contact_only[pitcher_df_filtered_contact_only['barrel']]
            .groupby('pitcher')
            .size()
    )
    
    launch_speed_sum = (
        pitcher_df_filtered_contact_only
            .groupby('pitcher')['launch_speed']
            .sum()
    )
    
    launch_angle_sum = (
        pitcher_df_filtered_contact_only
            .groupby('pitcher')['launch_angle']
            .sum()
    )

    # EV90 (90th percentile)
    ev90 = (
        pitcher_df_filtered_contact_only
            .groupby('pitcher')['launch_speed']
            .quantile(0.90)
    )
    
    xwoba_allowed = (
        statcast_df
        .groupby("pitcher")["estimated_woba_using_speedangle"]
        .sum()
    )
    
    league_xwoba = 0.3162979120429958
    
    league_era = 4.15
    
    
    ########################### add counts ###########################
    
    
    
    pitcher_df["pitches"] = pitches
    pitcher_df["pitches_in_zone"] = pitches_in_strike_zone
    pitcher_df["pitches_out_zone"] = pitches_outside_strike_zone
    
    pitcher_df["swings"] = swings
    pitcher_df["swings_in_zone"] = swings_in_strike_zone
    pitcher_df["swings_out_zone"] = swings_outside_strike_zone
    
    pitcher_df["contacted_balls"] = contacted_balls
    pitcher_df["contacted_balls_in_zone"] = contacted_balls_in_strike_zone
    pitcher_df["contacted_balls_out_zone"] = contacted_balls_outside_strike_zone
    
    pitcher_df["whiffs"] = whiffed_balls
    pitcher_df["whiffs_in_zone"] = whiffed_balls_in_strike_zone
    pitcher_df["whiffs_out_zone"] = whiffed_balls_outside_strike_zone
    
    pitcher_df["called_strikes"] = called_strikes
    
    pitcher_df["first_pitches"] = first_pitches
    pitcher_df["first_pitch_strikes"] = first_pitch_strikes
    
    pitcher_df["ground_balls"] = ground_balls
    pitcher_df["fly_balls"] = fly_balls
    pitcher_df["line_drives"] = line_drives
    pitcher_df["popups"] = popups
    
    pitcher_df["batted_balls"] = batted_balls
    pitcher_df["hard_hit_balls"] = hard_hit_balls
    pitcher_df["barrel_balls"] = barrel_balls
    pitcher_df["launch_speed_sum"] = launch_speed_sum
    pitcher_df["launch_angle_sum"] = launch_angle_sum
    
    pitcher_df['xWOBA_allowed'] = xwoba_allowed

    return(pitcher_df)

def calculate_pitcher_stats(pitcher_df):
    #pull in pitcher_seasonal_data from database

    pitcher_df = pitcher_df.drop(columns=['season'])
    
    pitcher_data_sums = (pitcher_df
             .groupby('xMLBAMID')
             .sum(numeric_only=True)
            )
    
    pitcher_data_sums['xMLBAMID'] = pitcher_data_sums.index
    
    pitcher_data_sums["batters_faced"] = (
        pitcher_data_sums["strikeouts"] +
        pitcher_data_sums["walks"] +
        pitcher_data_sums["hit_by_pitch"] +
        pitcher_data_sums["batted_balls"]
    )

    
    
    # --- Contact quality ---
    pitcher_data_sums["EV"] = pitcher_data_sums["launch_speed_sum"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["LA"] = pitcher_data_sums["launch_angle_sum"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["Hard%"] = pitcher_data_sums["hard_hit_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["Barrel%"] = pitcher_data_sums["barrel_balls"] / pitcher_data_sums["batted_balls"]
    
    # --- Batted-ball profile ---
    pitcher_data_sums["GB%"] = pitcher_data_sums["ground_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["FB%"] = pitcher_data_sums["fly_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["LD%"] = pitcher_data_sums["line_drives"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["HR/FB"] = pitcher_data_sums["home_runs"] / pitcher_data_sums["fly_balls"]
    
    # --- Plate discipline ---
    pitcher_data_sums["Zone%"] = pitcher_data_sums["pitches_in_zone"] / pitcher_data_sums["pitches"]
    pitcher_data_sums["Z-Swing%"] = pitcher_data_sums["swings_in_zone"] / pitcher_data_sums["pitches_in_zone"]
    pitcher_data_sums["O-Swing%"] = pitcher_data_sums["swings_out_zone"] / pitcher_data_sums["pitches_out_zone"]
    
    pitcher_data_sums["Contact%"] = pitcher_data_sums["contacted_balls"] / pitcher_data_sums["swings"]
    pitcher_data_sums["Z-Contact%"] = pitcher_data_sums["contacted_balls_in_zone"] / pitcher_data_sums["swings_in_zone"]
    pitcher_data_sums["O-Contact%"] = pitcher_data_sums["contacted_balls_out_zone"] / pitcher_data_sums["swings_out_zone"]

    
    pitcher_data_sums["SwStr%"] = pitcher_data_sums["whiffs"] / pitcher_data_sums["pitches"]
    pitcher_data_sums["CStr%"] = pitcher_data_sums["called_strikes"] / pitcher_data_sums["pitches"]
    pitcher_data_sums["C+SwStr%"] = (pitcher_data_sums["called_strikes"] + pitcher_data_sums["whiffs"]) / pitcher_data_sums["pitches"]
    
    pitcher_data_sums["F-Strike%"] = pitcher_data_sums["first_pitch_strikes"] / pitcher_data_sums["first_pitches"]
    
    # --- K/BB family ---
    pitcher_data_sums["K%"] = pitcher_data_sums["strikeouts"] / pitcher_data_sums["batters_faced"]
    pitcher_data_sums["BB%"] = pitcher_data_sums["walks"] / pitcher_data_sums["batters_faced"]
    pitcher_data_sums["K/BB"] = pitcher_data_sums["strikeouts"] / pitcher_data_sums["walks"]
    pitcher_data_sums["K-BB%"] = pitcher_data_sums["K%"] - pitcher_data_sums["BB%"]
    
    # --- Run prevention ---
    pitcher_data_sums["WHIP"] = (pitcher_data_sums["walks"] + pitcher_data_sums["hits"]) / pitcher_data_sums["IP"]
    pitcher_data_sums["BABIP"] = (pitcher_data_sums["hits"] - pitcher_data_sums["home_runs"]) / (pitcher_data_sums["batted_balls"] - pitcher_data_sums["home_runs"])
    
    # --- xERA ---
    pitcher_data_sums["xwOBA"] = pitcher_data_sums["xWOBA_allowed"] / pitcher_data_sums["batted_balls"]
    
    
    league_xwoba = 0.3162979120429958
        
    league_era = 4.15
    
    fip_constant = 3.1495185210234546
    
    
    # You supply league_xwOBA and league_ERA from your 2025 benchmark
    pitcher_data_sums["xERA"] = league_era + (pitcher_data_sums["xwOBA"] - league_xwoba) * 1.15 * 9
    
    pitcher_data_sums["FIP"] = (
        (13 * pitcher_data_sums["home_runs"] +
         3 * (pitcher_data_sums["walks"] + pitcher_data_sums["hit_by_pitch"]) -
         2 * pitcher_data_sums["strikeouts"]) / pitcher_data_sums["IP"]
    ) + fip_constant

        # Attach Throws after aggregation
    throws_lookup = (
        pitcher_df.groupby("xMLBAMID")["Throws"]
        .first()
    )
    
    pitcher_data_sums["Throws"] = pitcher_data_sums["xMLBAMID"].map(throws_lookup)

    return(pitcher_data_sums)

def process_starting_pitcher_current_year_stats(pitcher_statsapi_df, pitcher_statcast_df):

    combined_pitcher_df = pd.merge(pitcher_statsapi_df,
                                  pitcher_statcast_df,
                                  how='left',
                                  on=['xMLBAMID', 'season']
                                  )
    
    # FOR CURRENT SEASON ONLY
    current_year = datetime.now().year
    current_year_filter = combined_pitcher_df['season'] == current_year
    current_year_stats_df = combined_pitcher_df[current_year_filter].copy()
    
    # Compute stats
    league_xwoba = 0.3162979120429958
        
    league_era = 4.15
    
    fip_constant = 3.1495185210234546
    
    current_year_stats_df['inningsPitched'] = convert_ip(current_year_stats_df['inningsPitched'])
    
    current_year_stats_df["AVG"] = current_year_stats_df["hits"] / current_year_stats_df["atBats"]
    current_year_stats_df["WHIP"] = (
        current_year_stats_df["baseOnBalls"] + current_year_stats_df["hits"]
    ) / current_year_stats_df["inningsPitched"]
    
    current_year_stats_df["FIP"] = (
        (13 * current_year_stats_df["homeRuns"]) +
        (3 * current_year_stats_df["baseOnBalls"]) -
        (2 * current_year_stats_df["strikeOuts"])
    ) / current_year_stats_df["inningsPitched"] + fip_constant
    
    
    current_year_stats_df["ERA"] = (
        current_year_stats_df["earnedRuns"] / current_year_stats_df["inningsPitched"]
    ) * 9
    
    current_year_stats_df["BABIP"] = (
        current_year_stats_df["hits"] - current_year_stats_df["homeRuns"]
    ) / (
        current_year_stats_df["atBats"]
        - current_year_stats_df["strikeOuts"]
        - current_year_stats_df["homeRuns"]
        + current_year_stats_df["sacFlies"]
    )
    
    current_year_stats_df["LOB%"] = (
        (current_year_stats_df["hits"] +
         current_year_stats_df["baseOnBalls"] +
         current_year_stats_df["hitByPitch"] -
         current_year_stats_df["runs"])
        /
        (current_year_stats_df["hits"] +
         current_year_stats_df["baseOnBalls"] +
         current_year_stats_df["hitByPitch"] -
         1.4 * current_year_stats_df["homeRuns"])
    )

    current_year_stats_df["TTO%"] = (current_year_stats_df['homeRuns'] + current_year_stats_df['baseOnBalls'] + current_year_stats_df['strikeOuts']) / current_year_stats_df['atBats']

    pitcher_data_sums
    
    current_year_stats_df["RS/9"] = (
        current_year_stats_df["runs"] / current_year_stats_df["inningsPitched"]
    ) * 9
    
    current_year_stats_df["HR/9"] = (
        current_year_stats_df["homeRuns"] / current_year_stats_df["inningsPitched"]
    ) * 9
    
    current_year_stats_df["K/9"] = (
    current_year_stats_df["strikeOuts"] / current_year_stats_df["inningsPitched"]
    ) * 9

    current_year_stats_df["BF_per_start"] = (
    current_year_stats_df["battersFaced"] / current_year_stats_df["gamesStarted"]
    )

    current_year_stats_df["IP_per_start"] = (
    current_year_stats_df["inningsPitched"] / current_year_stats_df["gamesStarted"]
    )

    current_year_stats_df["DP%"] = (
    current_year_stats_df["groundIntoDoublePlay"] /
    (current_year_stats_df["battersFaced"]
     - current_year_stats_df["strikeOuts"]
     - current_year_stats_df["baseOnBalls"]
     - current_year_stats_df["hitByPitch"])
    )

    current_year_stats_df["GO/AO"] = (
    current_year_stats_df["groundOuts"] / current_year_stats_df["airOuts"]
    )

    current_year_stats_df["EV"] = current_year_stats_df["launch_speed_sum"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["LA"] = current_year_stats_df["launch_angle_sum"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["HardHit%"] = current_year_stats_df["hard_hit_balls"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["Med%"] = current_year_stats_df["med_hit_balls"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["Soft%"] = current_year_stats_df["soft_hit_balls"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["Barrel%"] = current_year_stats_df["barrel_balls"] / current_year_stats_df["batted_balls"]
    
    # --- Batted-ball profile ---
    current_year_stats_df["GB%"] = current_year_stats_df["ground_balls"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["FB%"] = current_year_stats_df["fly_balls"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["LD%"] = current_year_stats_df["line_drives"] / current_year_stats_df["batted_balls"]
    current_year_stats_df["IFFB%"] = current_year_stats_df["popups"] / current_year_stats_df["fly_balls"]
    current_year_stats_df["HR/FB"] = current_year_stats_df["homeRuns"] / current_year_stats_df["fly_balls"]
    
    # --- Plate discipline ---
    current_year_stats_df["Zone%"] = current_year_stats_df["pitches_in_zone"] / current_year_stats_df["pitches"]
    current_year_stats_df["Z-Swing%"] = current_year_stats_df["swings_in_zone"] / current_year_stats_df["pitches_in_zone"]
    current_year_stats_df["O-Swing%"] = current_year_stats_df["swings_out_zone"] / current_year_stats_df["pitches_out_zone"]
    
    current_year_stats_df["Contact%"] = current_year_stats_df["contacted_balls"] / current_year_stats_df["swings"]
    current_year_stats_df["Z-Contact%"] = current_year_stats_df["contacted_balls_in_zone"] / current_year_stats_df["swings_in_zone"]
    current_year_stats_df["O-Contact%"] = current_year_stats_df["contacted_balls_out_zone"] / current_year_stats_df["swings_out_zone"]
    
    current_year_stats_df["SwStr%"] = current_year_stats_df["whiffs"] / current_year_stats_df["pitches"]
    current_year_stats_df["CStr%"] = current_year_stats_df["called_strikes"] / current_year_stats_df["pitches"]
    current_year_stats_df["C+SwStr%"] = (current_year_stats_df["called_strikes"] + current_year_stats_df["whiffs"]) / current_year_stats_df["pitches"]
    
    current_year_stats_df["F-Strike%"] = current_year_stats_df["first_pitch_strikes"] / current_year_stats_df["first_pitches"]
    
    # --- K/BB family ---
    current_year_stats_df["K%"] = current_year_stats_df["strikeOuts"] / current_year_stats_df["battersFaced"]
    current_year_stats_df["BB%"] = current_year_stats_df["baseOnBalls"] / current_year_stats_df["battersFaced"]
    current_year_stats_df["K/BB"] = current_year_stats_df["strikeOuts"] / current_year_stats_df["baseOnBalls"]
    current_year_stats_df["K-BB%"] = current_year_stats_df["K%"] - current_year_stats_df["BB%"]
    
    current_year_stats_df["xwOBA"] = current_year_stats_df["xWOBA_allowed"] / current_year_stats_df["batted_balls"]
    
    # You supply league_xwOBA and league_ERA from your 2025 benchmark
    current_year_stats_df["xERA"] = league_era + (current_year_stats_df["xwOBA"] - league_xwoba) * 1.15 * 9
    
    final_columns = [
        "xMLBAMID", "player_name", "team_name", "age", "season",
        
        # Workload
        "gamesStarted", "inningsPitched", "battersFaced", 'IP_per_start', 'BF_per_start',
        
        # Results
        "wins", "losses", "ERA", "xERA", "FIP", "AVG", "WHIP", "BABIP", "RS/9", "H/9", "HR/9", "LOB%", "TTO%",
        
        # K/BB profile
        "K%", "BB%", "K/BB", "K-BB%", "K/9",
        
        # Contact quality
        "EV", "LA", "HardHit%", "Med%", "Soft%", "Barrel%", "GB%", "FB%", "LD%", "HR/FB", 'DP%', "IFFB%", 'GO/AO', 
        
        # Plate discipline
        "Zone%", "Z-Swing%", "O-Swing%", "Contact%", "Z-Contact%", "O-Contact%",
        "SwStr%", "CStr%", "C+SwStr%", "F-Strike%",
        
        # Baserunner control
        "wildPitches", "balks", "pickoffs", "caughtStealing", "stolenBases"
    ]
    
    final_display_df = current_year_stats_df[final_columns].copy()

    final_display_df['update date'] = datetime.now(pytz.timezone("America/New_York"))
    
    final_display_df = final_display_df.rename(columns={
        "gamesStarted": "GS",
        "inningsPitched": "IP",
        "battersFaced": "BF",
        "wins": "W",
        "losses": "L"
    })

    return final_display_df

def process_starting_pitcher_stats(pitcher_statsapi_df, pitcher_statcast_df):
    
    combined_pitcher_df = pd.merge(pitcher_statsapi_df,
                                   pitcher_statcast_df,
                                   how='left',
                                   on=['xMLBAMID', 'season']
                                  )
    
    combined_pitcher_df['inningsPitched'] = convert_ip(combined_pitcher_df['inningsPitched'])
    
    # Extract identity columns BEFORE grouping
    identity_cols = combined_pitcher_df[["xMLBAMID", "player_name", "Throws"]].drop_duplicates("xMLBAMID")
    
    # Group numeric stats
    pitcher_data_sums = (
        combined_pitcher_df
        .groupby("xMLBAMID")
        .sum(numeric_only=True)
        .reset_index()
    )
    
    # Merge identity back
    pitcher_data_sums = pitcher_data_sums.merge(identity_cols, on="xMLBAMID", how="left")
    
    # Compute stats
    league_xwoba = 0.3162979120429958
        
    league_era = 4.15
    
    fip_constant = 3.1495185210234546
    
    
    pitcher_data_sums["AVG"] = pitcher_data_sums["hits"] / pitcher_data_sums["atBats"]
    pitcher_data_sums["WHIP"] = (
        pitcher_data_sums["baseOnBalls"] + pitcher_data_sums["hits"]
    ) / pitcher_data_sums["inningsPitched"]
    
    pitcher_data_sums["FIP"] = (
        (13 * pitcher_data_sums["homeRuns"]) +
        (3 * pitcher_data_sums["baseOnBalls"]) -
        (2 * pitcher_data_sums["strikeOuts"])
    ) / pitcher_data_sums["inningsPitched"] + fip_constant
    
    
    pitcher_data_sums["ERA"] = (
        pitcher_data_sums["earnedRuns"] / pitcher_data_sums["inningsPitched"]
    ) * 9
    
    pitcher_data_sums["BABIP"] = (
        pitcher_data_sums["hits"] - pitcher_data_sums["homeRuns"]
    ) / (
        pitcher_data_sums["atBats"]
        - pitcher_data_sums["strikeOuts"]
        - pitcher_data_sums["homeRuns"]
        + pitcher_data_sums["sacFlies"]
    )
    
    pitcher_data_sums["LOB%"] = (
        (pitcher_data_sums["hits"] +
         pitcher_data_sums["baseOnBalls"] +
         pitcher_data_sums["hitByPitch"] -
         pitcher_data_sums["runs"])
        /
        (pitcher_data_sums["hits"] +
         pitcher_data_sums["baseOnBalls"] +
         pitcher_data_sums["hitByPitch"] -
         1.4 * pitcher_data_sums["homeRuns"])
    )

    pitcher_data_sums["TTO%"] = (pitcher_data_sums['homeRuns'] + pitcher_data_sums['baseOnBalls'] + pitcher_data_sums['strikeOuts']) / pitcher_data_sums['atBats']
    
    pitcher_data_sums["H/9"] = (
        pitcher_data_sums["hits"] / pitcher_data_sums["inningsPitched"]
    ) * 9

    pitcher_data_sums["BB/9"] = (
        pitcher_data_sums["baseOnBalls"] / pitcher_data_sums["inningsPitched"]
    ) * 9
    
    pitcher_data_sums["RS/9"] = (
        pitcher_data_sums["runs"] / pitcher_data_sums["inningsPitched"]
    ) * 9
    
    pitcher_data_sums["HR/9"] = (
        pitcher_data_sums["homeRuns"] / pitcher_data_sums["inningsPitched"]
    ) * 9
    
    pitcher_data_sums["K/9"] = (
    pitcher_data_sums["strikeOuts"] / pitcher_data_sums["inningsPitched"]
    ) * 9
    
    
    pitcher_data_sums["BF_per_start"] = np.where(
        pitcher_data_sums["gamesStarted"] == 0,
        0,
        pitcher_data_sums["battersFaced"] / pitcher_data_sums["gamesStarted"]
    )
    
    pitcher_data_sums["IP_per_start"] = np.where(
        pitcher_data_sums["gamesStarted"] == 0,
        0,
        pitcher_data_sums["inningsPitched"] / pitcher_data_sums["gamesStarted"]
    )
    
    
    pitcher_data_sums["DP%"] = (
    pitcher_data_sums["groundIntoDoublePlay"] /
    (pitcher_data_sums["battersFaced"]
     - pitcher_data_sums["strikeOuts"]
     - pitcher_data_sums["baseOnBalls"]
     - pitcher_data_sums["hitByPitch"])
    )
    
    pitcher_data_sums["GO/AO"] = (
    pitcher_data_sums["groundOuts"] / pitcher_data_sums["airOuts"]
    )
    
    pitcher_data_sums["EV"] = pitcher_data_sums["launch_speed_sum"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["LA"] = pitcher_data_sums["launch_angle_sum"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["HardHit%"] = pitcher_data_sums["hard_hit_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["Med%"] = pitcher_data_sums["med_hit_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["Soft%"] = pitcher_data_sums["soft_hit_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["Barrel%"] = pitcher_data_sums["barrel_balls"] / pitcher_data_sums["batted_balls"]
    
    # --- Batted-ball profile ---
    pitcher_data_sums["GB%"] = pitcher_data_sums["ground_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["FB%"] = pitcher_data_sums["fly_balls"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["LD%"] = pitcher_data_sums["line_drives"] / pitcher_data_sums["batted_balls"]
    pitcher_data_sums["IFFB%"] = pitcher_data_sums["popups"] / pitcher_data_sums["fly_balls"]
    pitcher_data_sums["HR/FB"] = pitcher_data_sums["homeRuns"] / pitcher_data_sums["fly_balls"]
    pitcher_data_sums["GB/FB"] = pitcher_data_sums["ground_balls"] / pitcher_data_sums["fly_balls"]
    
    # --- Plate discipline ---
    pitcher_data_sums["Zone%"] = pitcher_data_sums["pitches_in_zone"] / pitcher_data_sums["pitches"]
    pitcher_data_sums["Z-Swing%"] = pitcher_data_sums["swings_in_zone"] / pitcher_data_sums["pitches_in_zone"]
    pitcher_data_sums["O-Swing%"] = pitcher_data_sums["swings_out_zone"] / pitcher_data_sums["pitches_out_zone"]
    
    pitcher_data_sums["Contact%"] = pitcher_data_sums["contacted_balls"] / pitcher_data_sums["swings"]
    pitcher_data_sums["Z-Contact%"] = pitcher_data_sums["contacted_balls_in_zone"] / pitcher_data_sums["swings_in_zone"]
    pitcher_data_sums["O-Contact%"] = pitcher_data_sums["contacted_balls_out_zone"] / pitcher_data_sums["swings_out_zone"]

    pitcher_data_sums["Swing%"] = pitcher_data_sums["swings"] / pitcher_data_sums["pitches"]
    pitcher_data_sums["SwStr%"] = pitcher_data_sums["whiffs"] / pitcher_data_sums["pitches"]
    pitcher_data_sums["CStr%"] = pitcher_data_sums["called_strikes"] / pitcher_data_sums["pitches"]
    pitcher_data_sums["C+SwStr%"] = (pitcher_data_sums["called_strikes"] + pitcher_data_sums["whiffs"]) / pitcher_data_sums["pitches"]
    
    pitcher_data_sums["F-Strike%"] = pitcher_data_sums["first_pitch_strikes"] / pitcher_data_sums["first_pitches"]
    
    # --- K/BB family ---
    pitcher_data_sums["K%"] = pitcher_data_sums["strikeOuts"] / pitcher_data_sums["battersFaced"]
    pitcher_data_sums["BB%"] = pitcher_data_sums["baseOnBalls"] / pitcher_data_sums["battersFaced"]
    pitcher_data_sums["K/BB"] = pitcher_data_sums["strikeOuts"] / pitcher_data_sums["baseOnBalls"]
    pitcher_data_sums["K-BB%"] = pitcher_data_sums["K%"] - pitcher_data_sums["BB%"]
    
    pitcher_data_sums["xwOBA"] = pitcher_data_sums["xWOBA_allowed"] / pitcher_data_sums["batted_balls"]
    
    # You supply league_xwOBA and league_ERA from your 2025 benchmark
    pitcher_data_sums["xERA"] = league_era + (pitcher_data_sums["xwOBA"] - league_xwoba) * 1.15 * 9
    
    final_columns = [
        "xMLBAMID", 'player_name', 'Throws',
        
        # Workload
        "gamesStarted", "inningsPitched", "battersFaced", 'IP_per_start', 'BF_per_start',
        
        # Results
        "ERA", "xERA", "FIP", "AVG", "WHIP", "BABIP", "RS/9", "H/9", "HR/9", "LOB%",
        
        # K/BB profile
        "K%", "BB%", "K/BB", "K-BB%", "K/9",
        
        # Contact quality
        "EV", "LA", "HardHit%", "Med%", "Soft%", "Barrel%", "GB%", "FB%", "LD%", "HR/FB", 'DP%', "IFFB%", 'GO/AO', "GB/FB",
        
        # Plate discipline
        "Zone%", "Z-Swing%", "O-Swing%", "Contact%", "Z-Contact%", "O-Contact%",
        "SwStr%", "CStr%", "C+SwStr%", "F-Strike%",
        
        # Baserunner control
        "wildPitches", "balks", "pickoffs", "caughtStealing", "stolenBases"
    ]
    
    final_display_df = pitcher_data_sums[final_columns].copy()
    
    final_display_df['update date'] = datetime.now(pytz.timezone("America/New_York"))
    
    final_display_df = final_display_df.rename(columns={
        "gamesStarted": "GS",
        "inningsPitched": "IP",
        "battersFaced": "BF"
    })
    
    return final_display_df

def compute_count_stats_batter_statcast(statcast_df):
    
    batter_ids = statcast_df['batter'].unique()
    
    batter_df = pd.DataFrame(index=batter_ids)
    
    batter_df["xMLBAMID"] = batter_df.index

    stance_lookup = (
    statcast_df.groupby("batter")["stand"]
    .unique()                      # returns array of unique stances
    .apply(lambda x: "S" if len(x) > 1 else x[0])
    .rename("Stance")
    )

    batter_df["Stance"] = batter_df["xMLBAMID"].map(stance_lookup)
    
    season = statcast_df['game_year'].unique()
    
    batter_df['season'] = season[0]
    
    strike_zone = [1,2,3,4,5,6,7,8,9]
    
    swinging_strike_event_list = ['swinging_strike', 'swinging_strike_blocked']
    
    contact_event_list = ['foul', 'foul_tip', 'hit_into_play', 'foul_pitchout']
    
    strike_event_list = [
        'foul', 'foul_tip', 'hit_into_play', 'foul_pitchout',
        'swinging_strike', 'swinging_strike_blocked', 'called_strike'
    ]
    
    swing_event_list = contact_event_list + swinging_strike_event_list
    
    strike_out_event_list = ['strikeout', 'strikeout_double_play']
    
    walk_event_list = ['walk', 'intent_walk']
    
    out_event_list = [
        'grounded_into_double_play',
        'field_out',
        'force_out',
        'fielders_choice_out',
        'double_play',
        'triple_play',
        'sac_fly',
        'sac_fly_double_play',
        'strikeout',
        'strikeout_double_play'
    ]
    
    hit_event_list = [
        'single',
        'double',
        'triple',
        'home_run'
    ]

    non_ab_events = [
    'walk',
    'intent_walk',
    'hit_by_pitch',
    'sac_bunt',
    'sac_fly',
    'catcher_interf'
    ]

    ###### Calculate Raw Counts ######
    
    event_only = statcast_df[statcast_df['events'].notna()]

    
    pitches = (statcast_df
                     .groupby('batter')
                     .size()
                    )
    
    pitches_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone)]
                              .groupby('batter')
                              .size()
                             )
    
    pitches_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone)]
                              .groupby('batter')
                              .size()
                             )
    
    
    swings = (statcast_df[statcast_df['description'].isin(swing_event_list)]
                     .groupby('batter')
                     .size()
                    )
    
    swings_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swing_event_list)]
                              .groupby('batter')
                              .size()
                             )
    
    swings_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swing_event_list)]
                              .groupby('batter')
                              .size()
                             )
    
    contacted_balls = (statcast_df[statcast_df['description'].isin(contact_event_list)]
                     .groupby('batter')
                     .size()
                    )
    
    contacted_balls_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(contact_event_list)]
                     .groupby('batter')
                     .size()
                    )
    
    contacted_balls_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(contact_event_list)]
                     .groupby('batter')
                     .size()
                    )
    
    whiffed_balls = (statcast_df[statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('batter')
                     .size()
                    )
    
    whiffed_balls_in_strike_zone = (statcast_df[statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('batter')
                     .size()
                    )
    
    whiffed_balls_outside_strike_zone = (statcast_df[~statcast_df['zone'].isin(strike_zone) & statcast_df['description'].isin(swinging_strike_event_list)]
                     .groupby('batter')
                     .size()
                    )
    
    called_strikes = (statcast_df[statcast_df['description'] == 'called_strike']
                              .groupby('batter')
                              .size()
                             )
    
    
    
    
    ########### batted ball section ######################
    
    batter_df_filtered_contact_only = statcast_df[statcast_df['bb_type'].notna()].copy()
    
    line_drives = (batter_df_filtered_contact_only[batter_df_filtered_contact_only['bb_type'] == 'line_drive']
                   .groupby('batter')
                   .size()
                  )
    
    ground_balls = (batter_df_filtered_contact_only[batter_df_filtered_contact_only['bb_type'] == 'ground_ball']
                   .groupby('batter')
                   .size()
                  )
    
    fly_balls = (batter_df_filtered_contact_only[batter_df_filtered_contact_only['bb_type'] == 'fly_ball']
                   .groupby('batter')
                   .size()
                  )
    popups = (batter_df_filtered_contact_only[batter_df_filtered_contact_only['bb_type'] == 'popup']
                   .groupby('batter')
                   .size()
                  )
    
    batted_balls = (
        batter_df_filtered_contact_only
            .groupby('batter')
            .size()
    )
    
    hard_hit_balls = (
        batter_df_filtered_contact_only[batter_df_filtered_contact_only['launch_speed'] >= 95]
            .groupby('batter')
            .size()
    )
    
    exit_velo = batter_df_filtered_contact_only['launch_speed']
    launch_angle = batter_df_filtered_contact_only['launch_angle']
    
    min_launch_angle = 26 - (exit_velo - 98)
    max_launch_angle = 30 + (exit_velo - 98)
    
    min_launch_angle = min_launch_angle.clip(lower=8)
    max_launch_angle = max_launch_angle.clip(upper=50)
    
    batter_df_filtered_contact_only['barrel'] = (
        (exit_velo >= 98) &
        (launch_angle >= min_launch_angle) &
        (launch_angle <= max_launch_angle)
    )
    
    barrel_balls = (
        batter_df_filtered_contact_only[batter_df_filtered_contact_only['barrel']]
            .groupby('batter')
            .size()
    )
    
    launch_speed_sum = (
        batter_df_filtered_contact_only
            .groupby('batter')['launch_speed']
            .sum()
    )
    
    launch_angle_sum = (
        batter_df_filtered_contact_only
            .groupby('batter')['launch_angle']
            .sum()
    )
    
    
    
    ########################### add counts ###########################
    
    batter_df["pitches"] = pitches
    batter_df["pitches_in_zone"] = pitches_in_strike_zone
    batter_df["pitches_out_zone"] = pitches_outside_strike_zone
    batter_df["swings"] = swings
    batter_df["swings_in_zone"] = swings_in_strike_zone
    batter_df["swings_out_zone"] = swings_outside_strike_zone
    
    batter_df["contacted_balls"] = contacted_balls
    batter_df["contacted_balls_in_zone"] = contacted_balls_in_strike_zone
    batter_df["contacted_balls_out_zone"] = contacted_balls_outside_strike_zone
    
    batter_df["whiffs"] = whiffed_balls
    batter_df["whiffs_in_zone"] = whiffed_balls_in_strike_zone
    batter_df["whiffs_out_zone"] = whiffed_balls_outside_strike_zone
    
    batter_df["called_strikes"] = called_strikes
    
    batter_df["GB"] = ground_balls
    batter_df["FB"] = fly_balls
    batter_df["LD"] = line_drives
    batter_df["PU"] = popups
    
    batter_df["batted_balls"] = batted_balls
    batter_df["hard_hit_balls"] = hard_hit_balls
    batter_df["barrel_balls"] = barrel_balls
    batter_df["launch_speed_sum"] = launch_speed_sum
    batter_df["launch_angle_sum"] = launch_angle_sum

    return(batter_df)

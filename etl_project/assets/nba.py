import pandas as pd 
from etl_project.connectors.nba_api import NBAApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime, timezone, timedelta, date
import datetime as dt

def calculate_age(birth_date):
    current_date = date.today()
    return current_date.year - birth_date.year - ((current_date.month, current_date.day) < (birth_date.month, birth_date.day))

def extract_teams_in_league(nba_api_client: NBAApiClient, league: str) -> list:
    """
    Return a list of teamIDs from teams in a specified league
    """
    teams = nba_api_client.get_teams(league)
    team_ids = []
    for team in teams:
        team_ids.append(team['id'])
    return team_ids    

def extract_players(
        nba_api_client: NBAApiClient,
        season: int,
        league: str,
    )->pd.DataFrame:
    """
    Perform extraction of players into a pandas dataframe
    """
    data = []

    for team in extract_teams_in_league(nba_api_client=nba_api_client, league=league):
        data.extend(nba_api_client.get_players(season=season, team=team))

    df = pd.json_normalize(data=data)
    df["league"] = league
    df["season"] = season    
    return df

def extract_player_statistics(
        nba_api_client: NBAApiClient, 
        league: str,
        season: int
    )->pd.DataFrame:
    """
    Perform extraction of player statistics into a pandas dataframe
    """

    data = []

    for team in extract_teams_in_league(nba_api_client=nba_api_client, league=league):
        data.extend(nba_api_client.get_player_statistics(season=season, team=team))
    
    df = pd.json_normalize(data=data)
    df["league"] = league
    df["season"] = season    
    return df

def extract_standings(
        nba_api_client: NBAApiClient,
        league: str,
        season: int
    )->pd.DataFrame:
    """
    Perform extraction of standings into a pandas dataframe
    """

    data = nba_api_client.get_standings(league=league, season=season)

    df = pd.json_normalize(data=data)
    df["league"] = league
    df["season"] = season
    return df

def extract_games(
        nba_api_client: NBAApiClient,
        league: str,
        season: int
    )->pd.DataFrame:
    """
    Perform extraction of game into a pandas dataframe
    """

    data = nba_api_client.get_games(league=league, season=season)

    df=pd.json_normalize(data=data)
    df["league"] = league
    df["season"] = season

    return df

def transform_player_statistics(
        df_players:pd.DataFrame, 
        df_players_statistics:pd.DataFrame
    )->pd.DataFrame:
    """
    Using df result from extracting players and df result from extracting player statistics, transform to create final df
    """
    df_players_selected = df_players[['id','height.meters','weight.kilograms','birth.date','leagues.standard.jersey','season','league']]

    df_players_renamed = df_players_selected.rename(columns={
        "id": "player_id",
        "height.meters": "height_meters",
        "weight.kilograms": "weight_kilograms",
        "birth.date": "birth_date",
        "leagues.standard.jersey": "jersey_number"
    })
    
    df_players_statistics_selected = df_players_statistics[['player.id','player.firstname','player.lastname','team.id','team.name','pos','points','game.id','season','league']]

    df_player_statistics_renamed = df_players_statistics_selected.rename(columns={
        "player.id": "player_id",
        "player.firstname": "first_name",
        "player.lastname": "last_name",
        "team.id": "team_id",
        "team.name": "team_name",
        "pos": "position",
        "game.id": "game_id"
    })

    df_player_summary = pd.merge(
        left=df_players_renamed, 
        right=df_player_statistics_renamed, 
        left_on=["player_id","league","season"], 
        right_on=["player_id","league","season"]
    )

    df_player_summary['birth_date']= pd.to_datetime(df_player_summary['birth_date']).dt.date
    df_player_summary["current_age"] = df_player_summary['birth_date'].apply(calculate_age)
    
    return df_player_summary

def get_winner_id(
        row
    ):
    if row["home_team_score"] > row["away_team_score"]:
        return row["home_team_id"]
    else:
        return row["away_team_id"]
    
def get_loser_id(
        row
    ):
    if row["home_team_score"] < row["away_team_score"]:
        return row["home_team_id"]
    else:
        return row["away_team_id"]
    
def transform_games(
        df_games:pd.DataFrame
    )->pd.DataFrame:
    """
    Create final df for games data
    """
    df_games_selected = df_games[['id','league','season','date.start','teams.home.id','teams.home.name','scores.home.points','teams.visitors.id','teams.visitors.name','scores.visitors.points']]

    df_games_renamed = df_games_selected.rename(columns={
        "id": "game_id",
        "date.start": "date",
        "teams.home.id": "home_team_id",
        "teams.home.name": "home_team_name",
        "scores.home.points": "home_team_score",
        "teams.visitors.id": "away_team_id",
        "teams.visitors.name": "away_team_name",
        "scores.visitors.points": "away_team_score"
    })
    
    df_games_renamed['date']= pd.to_datetime(df_games_renamed['date']).dt.date
    df_games_renamed['winner_team_id'] = df_games_renamed.apply(lambda row: get_winner_id(row), axis=1)
    df_games_renamed['loser_team_id'] = df_games_renamed.apply(lambda row: get_loser_id(row), axis=1)

    return df_games_renamed

def transform_standings(
        df_standings:pd.DataFrame
    )->pd.DataFrame:
    """
    Create final df for standings data
    """
    df_standings_selected = df_standings[['team.id','team.name','league','season','conference.name','conference.rank','division.name','division.rank','win.total','loss.total']]
    df_standings_renamed = df_standings_selected.rename(columns={
        "team.id": "team_id", 
        "team.name": "team_name",
        "conference.name": "conference_name",
        'conference.rank': "conference_rank",
        "division.name": "division_name",
        "division.rank": "division_rank",
        "win.total": "win_total",
        "loss.total": "loss_total"

    })

    return df_standings_renamed

def load(
        df: pd.DataFrame,
        postgresql_client: PostgreSqlClient, 
        table: Table, 
        metadata: MetaData, 
        load_method: str = "overwrite"
    ) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient='records'),
            table=table,
            metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient='records'),
            table=table,
            metadata=metadata
        )
    elif load_method == "overwrite": 
        postgresql_client.overwrite(
            data=df.to_dict(orient='records'),
            table=table,
            metadata=metadata
        )
    else: 
        raise Exception("Please specify a correct load method: [insert, upsert, overwrite]")

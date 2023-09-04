import pandas as pd 
from etl_project.connectors.nba_api import NBAApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
#from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime, timezone, timedelta

def extract_teams_in_league(nba_api_client: NBAApiClient, league: str) -> list:
    """
    Return a list of teamIDs from teams in a specified league
    """
    teams = nba_api_client.get_teams(league)
    team_ids = []
    for team in teams:
        team_ids.append(team['id'])
    return team_ids    

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
    return df


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

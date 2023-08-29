from dotenv import load_dotenv
import os 
from etl_project.connectors.nba_api import NBAApiClient
import pandas as pd


load_dotenv()
API_KEY = os.environ.get("API_KEY")
nba_api_client = NBAApiClient(api_key=API_KEY)
response_data=pd.json_normalize(data=nba_api_client.get_games(league="standard",date="2022-03-09",season=2021))
#print(response_data)


nba_api_client = NBAApiClient(api_key=API_KEY)
standings_data=pd.json_normalize(data=nba_api_client.get_standings(league="standard",season=2022))
#print(standings_data)

players_data=pd.json_normalize(data=nba_api_client.get_players(season=2022, team=1))
#print(players_data)

player_stats_data=pd.json_normalize(data=nba_api_client.get_player_statistics(season=2022, team=1))
#print(player_stats_data)

team_data=pd.json_normalize(data=nba_api_client.get_teams(league="standard"))
print(team_data)
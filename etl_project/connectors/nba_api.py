import requests
from datetime import datetime
import pandas as pd

class NBAApiClient:

    def __init__(self, api_key: str):
        self.base_url = "https://v2.nba.api-sports.io"
        self.rapidapi_host = "v2.nba.api-sports.io"
        if api_key is None: 
            raise Exception("API key cannot be set to None.")
        self.api_key = api_key

    def get_games(self, league: str, date: str, season: int) -> list[dict]:
        """
        Get the games data for an indicated league on a specific date. 

        Args: 
            league: the league for which games data will be requested, possible values are: "africa" "orlando" "sacramento" "standard" "utah" "vegas"
            date: date in YYYY-MM-DD format
            season: the season the game was played in in YYYY format

        Returns: 
            A list of games for a given league on the given date in the given season
        
        Raises:
            Exception if response code is not 200. 
        """
        url = f"{self.base_url}/games/"
        params = {
            "date": date,
            "league": league,
            "season": season
        }
        headers = {
            "X-RAPIDAPI-KEY": self.api_key,
            "x-rapidapi-host": self.rapidapi_host
        }
        response = requests.get(url=url, params=params, headers=headers)
        if response.status_code == 200 and response.json() is not None: 
            return response.json()["response"]
        else: 
            raise Exception(f"Failed to extract data from NBA API. Status Code: {response.status_code}. Response: {response.text}")
        
    def get_teams(self, league: str) -> list[dict]:
        """
        Get the team data for an indicated league. 

        Args: 
            league: the league for which team data will be requested, possible values are: "africa" "orlando" "sacramento" "standard" "utah" "vegas" 

        Returns: 
            A list of teams for a given league
        
        Raises:
            Exception if response code is not 200. 
        """
        url = f"{self.base_url}/teams/"
        params = {
            "league": league
        }
        headers = {
            "X-RAPIDAPI-KEY": self.api_key,
            "x-rapidapi-host": self.rapidapi_host
        }
        response = requests.get(url=url, params=params, headers=headers)
        if response.status_code == 200 and response.json() is not None: 
            return response.json()["response"]
        else: 
            raise Exception(f"Failed to extract data from NBA API. Status Code: {response.status_code}. Response: {response.text}")
    
    def get_players(self, season: int, team: int) -> list[dict]:
        """
        Get the player data for an indicated season and team. 

        Args: 
            season: the season the game was played in in YYYY format
            team: the id of the team 

        Returns: 
            A list of players for a given season and team
        
        Raises:
            Exception if response code is not 200. 
        """
        url = f"{self.base_url}/players/"
        params = {
            "season": season,
            "team": team
        }
        headers = {
            "X-RAPIDAPI-KEY": self.api_key,
            "x-rapidapi-host": self.rapidapi_host
        }
        response = requests.get(url=url, params=params, headers=headers)
        if response.status_code == 200 and response.json() is not None: 
            return response.json()["response"]
        else: 
            raise Exception(f"Failed to extract data from NBA API. Status Code: {response.status_code}. Response: {response.text}")
        
    def get_player_statistics(self, season: int, team: int ) -> list[dict]:
        """
        Get the player statistics data for an indicated season. 

        Args: 
            season: the season the game was played in in YYYY format

        Returns: 
            A list of player statistics by game for a given season
        
        Raises:
            Exception if response code is not 200. 
        """
        url = f"{self.base_url}/players/statistics/"
        params = {
            "season": season,
            "team": team
        }
        headers = {
            "X-RAPIDAPI-KEY": self.api_key,
            "x-rapidapi-host": self.rapidapi_host
        }
        response = requests.get(url=url, params=params, headers=headers)
        if response.status_code == 200 and response.json() is not None: 
            return response.json()["response"]
        else: 
            raise Exception(f"Failed to extract data from NBA API. Status Code: {response.status_code}. Response: {response.text}")
        

    def get_standings(self, league: str, season: int) -> list[dict]:
        """
        Get the standings data for an indicated league and season year. 

        Args: 
            league: the league for which games data will be requested, possible values are: "africa" "orlando" "sacramento" "standard" "utah" "vegas"
            season: the season the game was played in in YYYY format

        Returns: 
            
        
        Raises:
            Exception if response code is not 200. 
        """
        url = f"{self.base_url}/standings/"
        params = {
            "league": league,
            "season": season
        }
        headers = {
            "X-RAPIDAPI-KEY": self.api_key,
            "x-rapidapi-host": self.rapidapi_host
        }
        response = requests.get(url=url, params=params, headers=headers)
        if response.status_code == 200 and response.json() is not None: 
            return response.json()["response"]
        else: 
            raise Exception(f"Failed to extract data from NBA API. Status Code: {response.status_code}. Response: {response.text}")




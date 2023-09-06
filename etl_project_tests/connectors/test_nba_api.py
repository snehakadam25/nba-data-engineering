from dotenv import load_dotenv
from etl_project.connectors.nba_api import NBAApiClient
import os 
import pytest

@pytest.fixture
def setup():
    load_dotenv()

def test_get_games(setup):
    API_KEY = os.environ.get("API_KEY")
    nba_client = NBAApiClient(api_key=API_KEY)
    data = nba_client.get_games(league="standard", season=2021)
    
    assert type(data) == list
    assert len(data) > 0

def test_get_standings(setup):
    API_KEY = os.environ.get("API_KEY")
    nba_client = NBAApiClient(api_key=API_KEY)
    data = nba_client.get_standings(league="standard", season=2022)

    assert type(data) == list
    assert len(data) > 0
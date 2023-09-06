from dotenv import load_dotenv
import os 
from etl_project.assets.nba import calculate_age, extract_teams_in_league, extract_standings, extract_games, extract_players, extract_player_statistics, \
    get_loser_id, get_winner_id, transform_games, transform_standings, transform_player_statistics, load
from etl_project.connectors.nba_api import NBAApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, Date
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
import logging
import yaml
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError

if __name__ == "__main__":
    #pipeline_logging = PipelineLogging(pipeline_name="nba", log_folder_path="etl_project/logs")
    load_dotenv()

    # Get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            config = pipeline_config.get("config")
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name.")

    pipeline_logging = PipelineLogging(pipeline_name=pipeline_config.get("name"), log_folder_path=config.get("log_folder_path"))
    
    # set up environment variables
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT
    )

    metadata_logger = MetaDataLogging(
        pipeline_name=PIPELINE_NAME, 
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config")
    )

    pipeline_logging.logger.info("Starting pipeline run")
    try:
        metadata_logger.log() #log start
        pipeline_logging.logger.info("Getting pipeline environment variables")
        API_KEY = os.environ.get("API_KEY")
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        SERVER_NAME = os.environ.get("SERVER_NAME")
        DATABASE_NAME = os.environ.get("DATABASE_NAME")
        PORT = os.environ.get("PORT")
        
        # Creating NBA API client
        pipeline_logging.logger.info("Creating NBA API client")
        nba_api_client = NBAApiClient(api_key=API_KEY)
        
        # Extracting data
        pipeline_logging.logger.info("Extracting data from NBA API client")
        pipeline_logging.logger.info("Extracting games data from NBA API client")
        df_games = extract_games(nba_api_client=nba_api_client, league=config.get("league"), season=config.get("season"))
        pipeline_logging.logger.info("Extracting players data from NBA API client")
        df_players = extract_players(nba_api_client=nba_api_client, league=config.get("league"), season=config.get("season"))
        pipeline_logging.logger.info("Extracting players statistics data from NBA API client")
        df_players_statistics = extract_player_statistics(nba_api_client=nba_api_client, league=config.get("league"), season=config.get("season"))
        pipeline_logging.logger.info("Extracting standings data from NBA API client")
        df_standings = extract_standings(nba_api_client=nba_api_client, league=config.get("league"), season=config.get("season"))

        # Transforming data
        pipeline_logging.logger.info("Transforming games df")
        df_games_transformed = transform_games(df_games=df_games)
        pipeline_logging.logger.info("Transforming players statistics")
        df_players_statistics_transformed = transform_player_statistics(df_players=df_players, df_players_statistics=df_players_statistics)
        pipeline_logging.logger.info("Transforming standings")
        df_standings_transformed = transform_standings(df_standings=df_standings)
        
        pipeline_logging.logger.info("Loading data to postgres")
        postgresql_client = PostgreSqlClient(
            server_name=SERVER_NAME,
            database_name=DATABASE_NAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            port=PORT
        )
        
        pipeline_logging.logger.info("Loading games data to postgres")
        metadata = MetaData()
        table_games = Table(
            "games", metadata, 
            Column("game_id", Integer, primary_key=True),
            Column("league", String),
            Column("season", Integer),
            Column("date", Date),
            Column("home_team_id", Integer),
            Column("home_team_name", String),
            Column("home_team_score", Float),
            Column("away_team_id", Integer),
            Column("away_team_name", String),
            Column("away_team_score", Float),
            Column("winner_team_id", Integer),
            Column("loser_team_id", Integer)
        )
        load(
            df=df_games_transformed,
            postgresql_client=postgresql_client, 
            table=table_games, 
            metadata=metadata,
            load_method="upsert"
        )

        pipeline_logging.logger.info("Loading standings data to postgres")
        
        table_standings = Table(
            "standings", metadata, 
            Column("team_id", Integer),
            Column("team_name", String),
            Column("league", String),
            Column("season", Integer),
            Column("conference_name", String),
            Column("conference_rank", Integer),
            Column("division_name", String),
            Column("division_rank", Integer),
            Column("win_total", Integer),
            Column("loss_total", Integer),
            Column("standings_table_id", String, primary_key=True)
        )
        load(
            df=df_standings_transformed,
            postgresql_client=postgresql_client, 
            table=table_standings, 
            metadata=metadata,
            load_method="upsert"
        )

        pipeline_logging.logger.info("Loading players statistics data to postgres")
        table_players_statistics = Table(
            "players_statistics", metadata,
            Column("player_id", Integer),
            Column("birth_date", Date),
            Column("jersey_number", Integer),
            Column("season", Integer),
            Column("league", String),
            Column("first_name", String),
            Column("last_name", String),
            Column("team_id", Integer),
            Column("position", String),
            Column("current_age", Integer),
            Column("points", Integer),
            Column("player_table_id", String)
        )
        load(
            df=df_players_statistics_transformed,
            postgresql_client=postgresql_client, 
            table=table_players_statistics, 
            metadata=metadata,
            load_method="overwrite"
        )

        metadata_logger.log(status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()) # log end
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()) # log error
        pipeline_logging.logger.handlers.clear()

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import logging
import psycopg2
import os

from dotenv import load_dotenv

load_dotenv()

URL = "https://api-basketball.p.rapidapi.com/standings"
HEADERS = {
    "content-type": "application/octet-stream",
    "X-RapidAPI-Key": "31f19fe571msh17c703e1e382172p132d20jsnf8e0c7337bd1",
    "X-RapidAPI-Host": "api-basketball.p.rapidapi.com"
}


@dataclass
class Paths:
    """path for file"""
    filename: str = "../raw_data/games.csv"
    sql_filname: str = "../models/verify_if_exist_from_db.sql"
    transformed_file: str = "../raw_data/transformed_data/transformed_file_car_sales.csv"
    log_dir: Path = Path("/logs")
    
    def __post_init__(self):
        self.base_path = "../raw_data"
        self.sql_file = "../models"
        self.data_source = Path(self.base_path) / "raw_data"
        self.data_transformed = Path(self.base_path) / "transformed_data"
        self.source_data = list(self.data_source.glob(self.filename))
        self.transformed_data = list(
            self.data_transformed.glob(self.transformed_file))


@dataclass
class Dates:
    """ date for log file or others"""
    start_date: date = date(2023, 3, 1)
    end_date: date = date(2023, 3, 1)
    
    def __post_init__(self):
        pass


def connect_to_db():
    # Connexion à la base de données
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USERNAME"),
            password=os.environ.get("DB_PASSWORD"))
        return conn
    except psycopg2.Error as e:
        logging.info(f"Error connecting to the database: {e}")
        return None


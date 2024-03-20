from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Union

import psycopg2


class Settings:
    # Database settings
    USER_NAME: str
    PASSWORD: str
    DB_NAME: str


URL: str
HEADERS = {
    "content-type": "application/octet-stream",
    "X-RapidAPI-Key": "31f19fe571msh17c703e1e382172p132d20jsnf8e0c7337bd1",
    "X-RapidAPI-Host": "api-basketball.p.rapidapi.com"
}


@dataclass
class Paths:
    """path for file"""
    filename: str = "../raw_data/games.csv"
    sql_filname: str = "verify_if_exist_from_db.sql"
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
            host="localhost",
            database=DB_NAME,
            user=USER_NAME,
            password=PASSWORD)
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None
    
    return conn

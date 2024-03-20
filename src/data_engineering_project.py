import csv
import glob as g
import json
from datetime import datetime

import pandas as pd
import psycopg2
import requests
from analytics_toolkit.sql import SQLReader
from psycopg2.extensions import AsIs


from config import HEADERS, URL, Paths, Settings, connect_to_db

output_data = []


class DataProcessor:

    def __init__(self):
        self.path = Paths()
        self.filename = self.path.filename
        self.transformed_file = self.path.transformed_file
        self.sql = SQLReader()
        self.settings = Settings()

    def extract_from_csv(self, file):
        # read the CSV file and select specific columns
        df = pd.read_csv(file)
        return df

    def data_from_db(self):
        sql_file = "../models/verify_if_exist_from_db.sql"
        read_query = self.sql.read_query(sql_file)
        return read_query

    def get_data_from_api(self):
        querystring = {"league": "12", "season": "2018-2019"}
        response = requests.get(URL, headers=HEADERS, params=querystring)
        nba_data = response.json()
        print(json.dumps(nba_data, indent=4))
        for data in nba_data["response"][0]:
            rang = data["position"]
            ligue_ses = data["stage"]
            conference = data["group"]["name"]
            equipe = data["team"]["name"]
            ligue = data["league"]["name"]
            saison = data["league"]["season"]
            pays = data["country"]["name"]
            pays_code = data["country"]["code"]
            match_joue = data["games"]["played"]
            match_win = data["games"]["win"]["total"]
            match_percentage_win = data["games"]["win"]["percentage"]
            match_lose = data["games"]["lose"]["total"]
            match_percentage_lose = data["games"]["lose"]["percentage"]

            output = {
                "rang": rang,
                "ligue_ses": ligue_ses,
                "conference": conference,
                "equipe": equipe,
                "ligue": ligue,
                "saison": saison,
                "pays": pays,
                "pays_code": pays_code,
                "match_joue": match_joue,
                "match_win": match_win,
                "match_percentage_win": match_percentage_win,
                "match_lose": match_lose,
                "match_percentage_lose": match_percentage_lose
            }
            output_data.append(output)
        df_from_api = pd.DataFrame(output_data).reset_index(drop=True)
        return df_from_api

    def transform_data(self, df):
        # Your code to transform the data goes here
        # rename the columns
        df = df.rename(columns={"Manufacturer": "marque",
                                "Sales_in_thousands": "quantite_vendue",
                                "Latest_Launch": "date_de_vente",
                                "Model": "model"})

        # convert the Sales_in_thousands column to numeric data type
        df["quantite_vendue"] = pd.to_numeric(df["quantite_vendue"])
        df['date_de_vente'] = pd.to_datetime(df['date_de_vente']).dt.date
        transformed_data = df
        # Write transformed data to CSV file
        transformed_data.to_csv(self.transformed_file, index=False)
        return transformed_data

    def send_to_database(self, data):
        conn = None
        try:
            conn = connect_to_db()
            cur = conn.cursor()

            for i, row in data.iterrows():
                cur.execute("../models/verify_if_exist_db.sql")
                result = cur.fetchone()
                print(result)
                if result:
                    print("Data already exists in the database.")
                else:
                    cur.execute("../models/insert_nba_games_data.sql")
            conn.commit()

            cur.close()
            conn.close()
            return True
        except psycopg2.DatabaseError as error:
            print(f"Erreur de connexion à la base de données: {error}")
        finally:
            if conn is not None:
                conn.close()

    def process_data(self):
        data = self.extract_from_csv()
        data_api = self.get_data_from_api()
        # transformed_data = self.transform_data(data)
        show_data = self.send_to_database(data_api)
        print(show_data)


if __name__ == '__main__':
    data_processor = DataProcessor()
    filepath = data_processor.filename
    data = data_processor.extract_from_csv(filepath)
    data_read = data_processor.data_from_db()
    print(data_read)
    # data_api = data_processor.get_data_from_api()

    # ts_data = data_processor.transform_data(data)
    data_processor.process_data()

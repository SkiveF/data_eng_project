import json
import pandas as pd
import requests
from config import URL, HEADERS

output_data = []


def extract_from_csv(self, file):
    # read the CSV file and select specific columns
    df = pd.read_csv(file).loc[:,
         ["Manufacturer", "Model", "Sales_in_thousands", "Latest_Launch"]]
    return df


def get_data_from_api():
    querystring = {"league": "12", "season": "2019-2020"}
    response = requests.get(URL, headers=HEADERS, params=querystring)
    nba_data = response.json()
    # print(json.dumps(nba_data, indent=4))
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
        match_percentage = data["games"]["lose"]["percentage"]

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
            "match_percentage": match_percentage
        }
        output_data.append(output)
    df_from_api = pd.DataFrame(output_data)
    return df_from_api

import pandas as pd
import glob as gb
from config import Paths

path = Paths()


# def climat_data(self, files):
# Utilisez glob pour obtenir la liste des fichiers CSV dans le dossier
liste_fichiers = gb.glob("../raw_data/meteo_data/" + '*.csv')
print(liste_fichiers)
# Créez une liste vide pour stocker les DataFrames de chaque fichier
liste_dataframes = []
# Parcourez la liste des fichiers et lisez-les un par un
for fichier in liste_fichiers:
    # Lire le fichier CSV
    data_frame = pd.read_csv(fichier)
    # Ajouter le DataFrame à la liste
    liste_dataframes.append(data_frame)
df_merged = pd.concat(liste_dataframes)
print(df_merged)


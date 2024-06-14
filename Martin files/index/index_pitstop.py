import pandas as pd
from elasticsearch import Elasticsearch, helpers
import urllib3
import numpy as np

# Désactiver les avertissements de sécurité
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Lire le fichier Parquet
file_path = r'D:\pitstop.parquet'
df = pd.read_parquet(file_path)
# Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Afficher le DataFrame complet
print(df)




# Initialiser le client Elasticsearch avec l'authentification de base
client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
    verify_certs=False
)

# Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
def pandas_df_to_elasticsearch(df, index_name):
    records = df.to_dict(orient='records')
    actions = [
        {
            "_index": index_name.lower(),
            "_source": record
        }
        for record in records
    ]
    helpers.bulk(client, actions)

# Nom de l'index Elasticsearch
index_name = "index_pitstop"

print("début de l'indéxation")


# Indexer les données
try:
    pandas_df_to_elasticsearch(df, index_name)
    print("Indexation réussie")
except helpers.BulkIndexError as e:
    print(f"Erreur lors de l'indexation : {e.errors}")

# Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
if client.indices.exists(index=index_name.lower()):
    print(f"L'index {index_name.lower()} existe.")
    count = client.count(index=index_name.lower())['count']
    print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
else:
    print(f"L'index {index_name.lower()} n'existe pas.")

import pandas as pd
from elasticsearch import Elasticsearch, helpers
import urllib3

# Désactiver les avertissements de sécurité
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Lire le fichier Parquet
file_path = r'D:\combined_data.parquet'
df = pd.read_parquet(file_path)

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
            "_index": index_name,
            "_source": record
        }
        for record in records
    ]
    helpers.bulk(client, actions)

# Nom de l'index Elasticsearch
index_name = "test_f1"

# Indexer les données
try:
    pandas_df_to_elasticsearch(df, index_name)
    print("Indexation réussie")
except helpers.BulkIndexError as e:
    print(f"Erreur lors de l'indexation : {e.errors}")

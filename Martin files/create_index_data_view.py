import requests
import json

# URL de votre instance Kibana
kibana_url = "http://localhost:5601"
username = "elastic"
password = "*TrARIRX=2kRhxsT8Ui7"

# Données pour créer un index pattern
index_pattern_data = {
    "attributes": {
        "title": "test_f1_V6",
        "timeFieldName": "@timestamp"  # Remplacez "@timestamp" par votre champ de temps si nécessaire
    }
}

# Requête pour créer l'index pattern
response = requests.post(
    f"{kibana_url}/api/saved_objects/index-pattern",
    headers={"kbn-xsrf": "true"},
    data=json.dumps(index_pattern_data),
    auth=(username, password)
)

# Afficher la réponse de Kibana
index_pattern_response = response.json()
print(index_pattern_response)

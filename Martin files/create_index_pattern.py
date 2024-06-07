import requests
import json

# URL de votre instance Kibana
kibana_url = "http://localhost:5601"
username = "elastic"
password = "*TrARIRX=2kRhxsT8Ui7"

# Données pour créer un index pattern
index_pattern_data = {
    "attributes": {
        "title": "test_f1",
        "timeFieldName": "@timestamp"  # Si vous avez un champ de temps, sinon omettez cette ligne
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
print(response.json())

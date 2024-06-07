import requests
import json

# URL de votre instance Kibana
kibana_url = "http://localhost:5601"
username = "elastic"
password = "*TrARIRX=2kRhxsT8Ui7"

# Données pour créer une visualisation
visualization_data = {
    "attributes": {
        "title": "Race Name Visualization",
        "visState": json.dumps({
            "title": "Race Name Visualization",
            "type": "pie",
            "params": {"addTooltip": True, "addLegend": True, "isDonut": False},
            "aggs": [
                {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
                {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {"field": "raceName.keyword", "size": 5, "order": "desc"}}
            ]
        }),
        "uiStateJSON": "{}",
        "description": "",
        "version": 1,
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"index": "test_f1", "query": {"language": "kuery", "query": ""}, "filter": []})}
    }
}

# Requête pour créer la visualisation
response = requests.post(
    f"{kibana_url}/api/saved_objects/visualization",
    headers={"kbn-xsrf": "true"},
    data=json.dumps(visualization_data),
    auth=(username, password)
)

# Afficher la réponse de Kibana
visualization_response = response.json()
print(visualization_response)

# Sauvegarder l'ID de la visualisation dans un fichier
with open("visualization_id.txt", "w") as f:
    f.write(visualization_response['id'])

import requests
import json

# URL de votre instance Kibana
kibana_url = "http://localhost:5601"
username = "elastic"
password = "*TrARIRX=2kRhxsT8Ui7"

# Charger l'ID de la visualisation depuis le fichier
with open("visualization_id.txt", "r") as f:
    visualization_id = f.read().strip()

# Données pour créer un tableau de bord
dashboard_data = {
    "attributes": {
        "title": "Race Name Dashboard",
        "hits": 0,
        "description": "",
        "panelsJSON": "[]",
        "optionsJSON": json.dumps({
            "useMargins": True,
            "hidePanelTitles": False
        }),
        "version": 1,
        "timeRestore": False,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "filter": [],
                "query": {
                    "language": "kuery",
                    "query": ""
                }
            })
        }
    }
}

# Requête pour créer le tableau de bord
response = requests.post(
    f"{kibana_url}/api/saved_objects/dashboard",
    headers={"kbn-xsrf": "true"},
    data=json.dumps(dashboard_data),
    auth=(username, password)
)

# Afficher la réponse de Kibana
dashboard_response = response.json()
print(dashboard_response)

# Récupérer l'ID du tableau de bord
dashboard_id = dashboard_response['id']

# Données pour mettre à jour le tableau de bord avec la visualisation
update_dashboard_data = {
    "attributes": {
        "title": "Race Name Dashboard",
        "hits": 0,
        "description": "",
        "panelsJSON": json.dumps([{
            "embeddableConfig": {},
            "gridData": {
                "h": 15,
                "i": "1",
                "w": 24,
                "x": 0,
                "y": 0
            },
            "id": visualization_id,
            "panelIndex": "1",
            "type": "visualization",
            "version": "7.3.0"
        }]),
        "optionsJSON": json.dumps({
            "useMargins": True,
            "hidePanelTitles": False
        }),
        "version": 1,
        "timeRestore": False,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({
                "filter": [],
                "query": {
                    "language": "kuery",
                    "query": ""
                }
            })
        }
    }
}

# Requête pour mettre à jour le tableau de bord
response = requests.put(
    f"{kibana_url}/api/saved_objects/dashboard/{dashboard_id}",
    headers={"kbn-xsrf": "true"},
    data=json.dumps(update_dashboard_data),
    auth=(username, password)
)

# Afficher la réponse de Kibana
print(response.json())

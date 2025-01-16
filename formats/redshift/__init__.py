import requests

def get_entities_config():
    r = requests.get('https://api.openalex.org/entities/config')
    r.raise_for_status()
    return r.json()

all_entities_config = get_entities_config()
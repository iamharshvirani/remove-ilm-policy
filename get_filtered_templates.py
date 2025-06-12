#!/usr/bin/env python3
import requests
import json
from requests.auth import HTTPBasicAuth

# Config
ES_HOST = 'http://localhost:9200'   # change as needed
ES_USER = 'your_user'
ES_PASSWORD = 'your_password'

def get_composable_templates():
    url = f"{ES_HOST}/_index_template"
    resp = requests.get(url, auth=HTTPBasicAuth(ES_USER, ES_PASSWORD))
    resp.raise_for_status()
    return resp.json().get("index_templates", [])

def find_templates_with_ilm(templates):
    with_ilm = []
    for entry in templates:
        name = entry['name']
        settings = entry['index_template'].get("template", {}).get("settings", {})
        lifecycle = settings.get("index", {}).get("lifecycle", {}).get("name")
        if lifecycle:
            with_ilm.append(name)
    return with_ilm

def main():
    templates = get_composable_templates()
    ilm_templates = find_templates_with_ilm(templates)
    print("Templates with ILM policy attached:")
    for name in ilm_templates:
        print(f"- {name}")

if __name__ == '__main__':
    main()

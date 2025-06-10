#!/usr/bin/env python3
import os
import re
import json
import argparse
import subprocess
from datetime import datetime
from getpass import getpass
from pathlib import Path
from dotenv import load_dotenv

# ------------------------------------------------
# Load environment variables
# ------------------------------------------------
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configuration
OLD_FORMAT_PATTERN = re.compile(r".*_c\d{3}_.*")
DRY_RUN_FILE = "ilm_template_removal_plan.txt"

# Environment defaults
ES_HOST = os.getenv('ES_HOST', 'localhost')
ES_PORT = int(os.getenv('ES_PORT', '9200'))
ES_USER = os.getenv('ES_USER', None)
ES_PASSWORD = os.getenv('ES_PASSWORD', None)
REPORT_DETAILS = os.getenv('REPORT_DETAILS', 'false').lower() in ('1','true','yes')


def curl_request(host, port, user, password, method, path, data=None):
    url = f"http://{host}:{port}{path}"
    cmd = ["curl", "-s", "-u", f"{user}:{password}", "-H", "Content-Type: application/json", "-H", "Accept: application/json", "-X", method]
    if data is not None:
        cmd += ["-d", json.dumps(data)]
    cmd.append(url)
    try:
        out = subprocess.check_output(cmd)
        return json.loads(out)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] curl failed on {path}: {e}")
    except json.JSONDecodeError:
        print(f"[ERROR] Cannot parse JSON response from {path}")
    return None


def scan_templates(host, port, user, password):
    """Fetch composable and legacy templates, return matching and excluded."""
    matched = []
    excluded = []
    # Composable templates
    comp = curl_request(host, port, user, password, "GET", "/_index_template")
    if comp and 'index_templates' in comp:
        for entry in comp['index_templates']:
            name = entry['name']
            patterns = entry['index_template'].get('index_patterns', [])
            if any(OLD_FORMAT_PATTERN.match(p) for p in patterns):
                matched.append(('composable', name))
            else:
                excluded.append(name)
    # Legacy templates
    legacy = curl_request(host, port, user, password, "GET", "/_template")
    if legacy:
        for name, body in legacy.items():
            patterns = body.get('index_patterns', [])
            if any(OLD_FORMAT_PATTERN.match(p) for p in patterns):
                matched.append(('legacy', name))
            else:
                excluded.append(name)
    return matched, excluded


def remove_lifecycle_from_template(host, port, user, password, ttype, name):
    """GET template, strip ILM settings, PUT back."""
    if ttype == 'composable':
        path = f"/_index_template/{name}"
        resp = curl_request(host, port, user, password, "GET", path)
        tmpl = resp['index_templates'][0]['index_template']
        # Remove ILM settings
        settings = tmpl.get('template', {}).get('settings', {})
        settings.get('index', {}).pop('lifecycle', None)
        # PUT back full template body
        data = tmpl
        return 'PUT', path, data
    else:
        path = f"/_template/{name}"
        resp = curl_request(host, port, user, password, "GET", path)
        tmpl = resp[name]
        settings = tmpl.get('settings', {})
        settings.get('index', {}).pop('lifecycle', None)
        # Legacy body includes settings, mappings, aliases
        data = {
            'index_patterns': tmpl.get('index_patterns', []),
            'settings': settings,
            'mappings': tmpl.get('mappings', {}),
            'aliases': tmpl.get('aliases', {})
        }
        return 'PUT', path, data


def generate_dry_run_plan(templates, host, port):
    if not templates:
        print("No matching templates found. No plan created.")
        return
    print(f"\nWriting dry-run plan to {DRY_RUN_FILE}…")
    with open(DRY_RUN_FILE, 'w') as f:
        f.write(f"# Plan generated on: {datetime.now().isoformat()}\n")
        f.write(f"# ES host: {host}:{port}\n# Commands: GET then PUT without ILM settings\n\n")
        for ttype, name in templates:
            method, path, data = remove_lifecycle_from_template(host, port, ES_USER, ES_PASSWORD, ttype, name)
            # GET
            f.write(f"# GET {path}\n")
            f.write(f"curl -s -u {ES_USER}:<password> -H 'Accept: application/json' http://{host}:{port}{path}\n\n")
            # PUT
            body = json.dumps(data)
            f.write(f"# PUT {path}\n")
            f.write(f"curl -X PUT -u {ES_USER}:<password> -H 'Content-Type: application/json' \
")
            f.write(f"    http://{host}:{port}{path} -d '{body}'\n\n")
    print("✅ Dry-run plan written.")


def execute_removal(templates, host, port):
    if not templates:
        print("No matching templates to update.")
        return
    print("\n--- EXECUTE MODE ---")
    confirm = input("Type 'proceed' to remove ILM from these templates: ")
    if confirm.strip().lower() != 'proceed':
        print("Aborted.")
        return
    for ttype, name in templates:
        method, path, data = remove_lifecycle_from_template(host, port, ES_USER, ES_PASSWORD, ttype, name)
        print(f"Updating {ttype} template '{name}'…", end=' ')
        resp = curl_request(host, port, ES_USER, ES_PASSWORD, method, path, data)
        print("OK" if resp is not None else "FAIL")


def main():
    parser = argparse.ArgumentParser(description="Freeze data streams by removing ILM from templates")
    parser.add_argument('--host', default=ES_HOST)
    parser.add_argument('--port', type=int, default=ES_PORT)
    parser.add_argument('--user', default=ES_USER)
    parser.add_argument('--password', default=ES_PASSWORD)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--dry-run', action='store_true', default=True)
    group.add_argument('--execute', action='store_true')
    args = parser.parse_args()

    if args.user and not args.password:
        args.password = getpass(f"Password for '{args.user}': ")

    templates, excluded = scan_templates(args.host, args.port, args.user, args.password)
    if REPORT_DETAILS:
        print(f"\n[REPORT] Found {len(templates)} matching templates, {len(excluded)} excluded templates")

    if args.execute:
        execute_removal(templates, args.host, args.port)
    else:
        generate_dry_run_plan(templates, args.host, args.port)

if __name__ == '__main__':
    main()

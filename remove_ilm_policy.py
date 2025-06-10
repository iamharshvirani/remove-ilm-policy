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


def curl_request(host, port, user, password, method, path, data=None, timeout=10):
    url = f"http://{host}:{port}{path}"
    cmd = ["curl", "-s", "--max-time", str(timeout), "-u", f"{user}:{password}",
           "-H", "Content-Type: application/json", "-H", "Accept: application/json", "-X", method]
    if data is not None:
        cmd += ["-d", json.dumps(data)]
    cmd.append(url)

    try:
        print(f"[DEBUG] Running curl: {' '.join(cmd)}")  # Optional: Log the curl command
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        return json.loads(out)
    except subprocess.CalledProcessError as e:
        error_output = e.output.decode().strip()
        if not error_output:
            error_message = (
                f"Connection to http://{host}:{port} failed. "
                "Please ensure Elasticsearch is running and accessible."
            )
        else:
            error_message = error_output
        print(f"[ERROR] curl failed on {path} (exit code {e.returncode}):\n{error_message}")
    except json.JSONDecodeError:
        print(f"[ERROR] Cannot parse JSON response from {path}")
    except Exception as ex:
        print(f"[ERROR] Unexpected error in curl_request: {ex}")
    return None



def template_has_lifecycle(host, port, user, password, name):
    """Return True if the composable template has ILM settings"""
    path = f"/_index_template/{name}"
    resp = curl_request(host, port, user, password, "GET", path)
    if not resp or 'index_templates' not in resp:
        return False
    tmpl = resp['index_templates'][0]['index_template']
    settings = tmpl.get('template', {}).get('settings', {})
    return 'lifecycle' in settings.get('index', {})


def scan_templates(host, port, user, password):
    """Fetch composable templates, filter by pattern and ILM presence"""
    candidates = []
    skipped = []
    comp = curl_request(host, port, user, password, "GET", "/_index_template")
    if comp and 'index_templates' in comp:
        for entry in comp['index_templates']:
            name = entry['name']
            patterns = entry['index_template'].get('index_patterns', [])
            if any(OLD_FORMAT_PATTERN.match(p) for p in patterns):
                if template_has_lifecycle(host, port, user, password, name):
                    candidates.append(name)
                else:
                    skipped.append(name)
    return candidates, skipped


def remove_lifecycle_from_template(host, port, user, password, name):
    """GET composable template, strip ILM settings, PUT back."""
    path = f"/_index_template/{name}"
    resp = curl_request(host, port, user, password, "GET", path)
    tmpl = resp['index_templates'][0]['index_template']
    # Remove ILM settings
    tmpl.get('template', {}).get('settings', {}).get('index', {}).pop('lifecycle', None)
    # Return PUT data
    return 'PUT', path, tmpl


def generate_dry_run_plan(templates, host, port):
    if not templates:
        print("No matching templates with ILM found. No plan created.")
        return
    print(f"\nWriting dry-run plan to {DRY_RUN_FILE}…")
    with open(DRY_RUN_FILE, 'w') as f:
        f.write(f"# Plan generated on: {datetime.now().isoformat()}\n")
        f.write(f"# ES host: {host}:{port}\n# Commands: GET then PUT without ILM settings\n\n")
        for name in templates:
            # GET command
            f.write(f"# GET /_index_template/{name}\n")
            f.write(f"curl -s -u {ES_USER}:<password> -H 'Accept: application/json' \
")
            f.write(f"    http://{host}:{port}/_index_template/{name}\n\n")
            # PUT command
            method, path, data = remove_lifecycle_from_template(host, port, ES_USER, ES_PASSWORD, name)
            body = json.dumps(data)
            f.write(f"# PUT {path}\n")
            f.write(f"curl -X PUT -u {ES_USER}:<password> -H 'Content-Type: application/json' \
")
            f.write(f"    http://{host}:{port}{path} -d '{body}'\n\n")
    print("✅ Dry-run plan written.")


def execute_removal(templates, host, port):
    if not templates:
        print("No matching templates with ILM to update.")
        return
    print("\n--- EXECUTE MODE ---")
    if input("Type 'proceed' to remove ILM from these templates: ").strip().lower() != 'proceed':
        print("Aborted.")
        return
    for name in templates:
        method, path, data = remove_lifecycle_from_template(host, port, ES_USER, ES_PASSWORD, name)
        print(f"Updating template '{name}'…", end=' ')
        resp = curl_request(host, port, ES_USER, ES_PASSWORD, method, path, data)
        print("OK" if resp else "FAIL")


def main():
    parser = argparse.ArgumentParser(description="Freeze data streams by removing ILM from composable templates")
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

    templates, skipped = scan_templates(args.host, args.port, args.user, args.password)
    if REPORT_DETAILS:
        print(f"\n[REPORT] Will update {len(templates)} templates; skipped {len(skipped)} without ILM settings")
        for name in skipped:
            print(f"  - {name}")

    if args.execute:
        execute_removal(templates, args.host, args.port)
    else:
        generate_dry_run_plan(templates, args.host, args.port)


if __name__ == '__main__':
    main()

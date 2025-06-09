#!/usr/bin/env python3
import os
import re
import json
import argparse
import subprocess
from datetime import datetime
from getpass import getpass
from pathlib import Path

# Optional: install python-dotenv with `pip install python-dotenv`
from dotenv import load_dotenv

# ------------------------------------------------
# Load environment variables from a .env file
# ------------------------------------------------
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# --- Configuration ---
OLD_FORMAT_PATTERN = re.compile(r".*_c\d{3}_.*")
DRY_RUN_FILE = "ilm_removal_plan.txt"

# Read defaults from env
ES_HOST = os.getenv('ES_HOST', 'localhost')
ES_PORT = int(os.getenv('ES_PORT', '9200'))
ES_USER = os.getenv('ES_USER', None)
ES_PASSWORD = os.getenv('ES_PASSWORD', None)


def curl_request(host, port, user, password, method, path):
    """
    Runs a curl command and returns parsed JSON (or None on failure).
    """
    url = f"http://{host}:{port}{path}"
    cmd = [
        "curl", "-s",
        "-u", f"{user}:{password}",
        "-H", "Content-Type: application/json",
        "-H", "Accept: application/json",
        "-X", method,
        url
    ]
    try:
        output = subprocess.check_output(cmd)
        return json.loads(output)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] curl failed: {e}")
    except json.JSONDecodeError:
        print("[ERROR] Failed to parse JSON from Elasticsearch response")
    return None


def get_target_backing_indices(host, port, user, password):
    print("Searching for data streams with the old format pattern…")
    data = curl_request(host, port, user, password, "GET", "/_data_stream?expand_wildcards=all")
    if not data or "data_streams" not in data:
        print("[ERROR] Could not fetch data streams.")
        return []
    targets = []
    for ds in data["data_streams"]:
        if OLD_FORMAT_PATTERN.match(ds["name"]):
            print(f"  [MATCH] {ds['name']}")
            for idx in ds["indices"]:
                targets.append(idx["index_name"])
    return targets


def generate_dry_run_plan(indices, host, port):
    if not indices:
        print("No matching indices found. Dry run file will not be created.")
        return
    print("\nGenerating dry run plan (no changes will be made)…")
    with open(DRY_RUN_FILE, "w") as f:
        f.write(f"# Dry run plan generated on: {datetime.now().isoformat()}\n")
        f.write(f"# Elasticsearch: http://{host}:{port}\n")
        f.write(f"# Found {len(indices)} indices matching old format\n")
        f.write("# Commands to remove ILM policy:\n\n")
        for idx in indices:
            f.write(f"curl -X POST http://{host}:{port}/{idx}/_ilm/remove -u {ES_USER}:<password>\n")
    print(f"Dry run complete. See {DRY_RUN_FILE}")


def execute_ilm_removal(host, port, user, password, indices):
    if not indices:
        print("No matching indices to operate on.")
        return
    print("\n--- EXECUTE MODE ---")
    for idx in indices:
        print(f"  - {idx}")
    confirm = input("\nType 'proceed' to remove ILM from all above: ")
    if confirm.strip().lower() != "proceed":
        print("Aborted.")
        return

    successes = failures = 0
    for idx in indices:
        print(f"Removing ILM from {idx}…", end=" ")
        result = curl_request(host, port, user, password, "POST", f"/{idx}/_ilm/remove")
        if result is not None:
            print("OK")
            successes += 1
        else:
            print("FAIL")
            failures += 1

    print(f"\nDone: {successes} succeeded, {failures} failed.")


def main():
    parser = argparse.ArgumentParser(
        description="Remove ILM from old-format data-stream backing indices"
    )
    parser.add_argument("--host", default=ES_HOST, help="Elasticsearch host")
    parser.add_argument("--port", type=int, default=ES_PORT, help="Elasticsearch port")
    parser.add_argument("--user", default=ES_USER, help="Elasticsearch username")
    parser.add_argument("--password", default=ES_PASSWORD, help="Elasticsearch password")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True)
    group.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    # Prompt if missing
    if args.user and not args.password:
        args.password = getpass(f"Password for '{args.user}': ")

    indices = get_target_backing_indices(args.host, args.port, args.user, args.password)
    if args.execute:
        execute_ilm_removal(args.host, args.port, args.user, args.password, indices)
    else:
        generate_dry_run_plan(indices, args.host, args.port)


if __name__ == "__main__":
    main()

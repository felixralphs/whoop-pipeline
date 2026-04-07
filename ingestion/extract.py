import os
import json
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

TOKEN_FILE = "token.json"
BASE_URL = "https://api.prod.whoop.com/developer/v2"

def load_tokens():
    with open(TOKEN_FILE, "r") as f:
        return json.load(f)

def get_headers():
    tokens = load_tokens()
    return {"Authorization": f"Bearer {tokens['access_token']}"}

def fetch_all_pages(endpoint):
    """Handles WHOOP pagination automatically"""
    results = []
    next_token = None

    while True:
        params = {"limit": 25}
        if next_token:
            params["nextToken"] = next_token

        response = requests.get(
            f"{BASE_URL}{endpoint}",
            headers=get_headers(),
            params=params
        )

        if response.status_code == 429:
            print("  Rate limit hit, waiting 60 seconds...")
            time.sleep(60)
            continue

        response.raise_for_status()
        data = response.json()

        results.extend(data.get("records", []))
        next_token = data.get("next_token")

        print(f"  Fetched {len(results)} records so far...")

        if not next_token:
            break

        time.sleep(0.7)

    return results

def extract_recovery():
    print("Extracting recovery data (via cycles)...")
    data = fetch_all_pages("/cycle")
    recovery_data = [c for c in data if c.get("recovery") is not None]
    print(f"  Retrieved {len(recovery_data)} recovery records")
    return recovery_data

def extract_sleep():
    print("Extracting sleep data...")
    data = fetch_all_pages("/activity/sleep")
    print(f"  Retrieved {len(data)} sleep records")
    return data

def extract_cycles():
    print("Extracting cycle data...")
    data = fetch_all_pages("/cycle")
    print(f"  Retrieved {len(data)} cycle records")
    return data

def extract_workouts():
    print("Extracting workout data...")
    data = fetch_all_pages("/activity/workout")
    print(f"  Retrieved {len(data)} workout records")
    return data

def save_raw(data, name):
    """Save raw JSON locally before loading to GCS"""
    os.makedirs("raw", exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = f"raw/{name}_{timestamp}.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Saved to {filepath}")
    return filepath

if __name__ == "__main__":
    print("Starting WHOOP data extraction...")
    print("="*40)

    recovery = extract_recovery()
    sleep = extract_sleep()
    cycles = extract_cycles()
    workouts = extract_workouts()

    print("="*40)
    print("Saving raw data locally...")

    save_raw(recovery, "recovery")
    save_raw(sleep, "sleep")
    save_raw(cycles, "cycles")
    save_raw(workouts, "workouts")

    print("="*40)
    print("Extraction complete!")
import os
import requests
import time
from supabase import create_client, Client

# 1. Setup Credentials
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
bdl_api_key = os.environ.get("BDL_API_KEY")
supabase: Client = create_client(url, key)

def sync_nba_teams():
    print("--- Starting NBA Teams Sync ---")
    headers = {"Authorization": bdl_api_key}
    response = requests.get("https://api.balldontlie.io/v1/teams", headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching teams: {response.status_code}")
        return

    teams = response.json()['data']
    for team in teams:
        conf = team.get('conference', 'N/A')
        div = team.get('division', 'N/A')
        
        supabase.table("teams").upsert({
            "api_id": str(team['id']),
            "full_name": team['full_name'],
            "abbreviation": team['abbreviation'],
            "conference_division": f"{conf}/{div}"
        }, on_conflict="api_id").execute()
    print("NBA Teams Sync Complete.")

def sync_current_nba_players():
    print("--- Starting Current NBA Players Sync ---")
    headers = {"Authorization": bdl_api_key}
    
    # Only map the 30 active teams (IDs 1-30)
    teams_query = supabase.table("teams").select("id, api_id").lte("api_id", "30").execute()
    team_map = {t['api_id']: t['id'] for t in teams_query.data}
    active_team_ids = list(team_map.keys())

    cursor = None
    while True:
        url = "https://api.balldontlie.io/v1/players?per_page=100"
        for t_id in active_team_ids:
            url += f"&team_ids[]={t_id}"
            
        if cursor:
            url += f"&cursor={cursor}"
            
        response = requests.get(url, headers=headers)
        
        if response.status_code == 429:
            print("Rate limited. Sleeping for 60s...")
            time.sleep(60)
            continue
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            break

        data = response.json()
        players = data.get('data', [])
        if not players:
            break

        players_to_upsert = []
        for p in players:
            api_team_id = str(p['team']['id'])
            if api_team_id in team_map:
                players_to_upsert.append({
                    "api_id": str(p['id']),
                    "team_id": team_map[api_team_id],
                    "name": f"{p['first_name']} {p['last_name']}",
                    "position": p.get('position', 'N/A'),
                    "is_active": True
                })

        if players_to_upsert:
            supabase.table("players").upsert(players_to_upsert, on_conflict="api_id").execute()
            print(f"Batch synced {len(players_to_upsert)} current players.")

        cursor = data.get('meta', {}).get('next_cursor')
        if not cursor:
            break
    print("NBA Players Sync Complete.")

if __name__ == "__main__":
    sync_nba_teams()
    sync_current_nba_players()
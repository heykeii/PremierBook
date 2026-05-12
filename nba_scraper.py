import os
import requests
import time
from datetime import datetime
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
    print("--- Syncing ONLY Active NBA Roster ---")
    headers = {"Authorization": bdl_api_key}
    
    current_season = datetime.utcnow().year if datetime.utcnow().month >= 10 else datetime.utcnow().year - 1

    # 1. Map the current NBA teams we already synced.
    teams_query = supabase.table("teams").select("id, api_id").execute()
    team_map = {t['api_id']: t['id'] for t in teams_query.data}

    if not team_map:
        print("No teams found in Supabase. Run team sync first.")
        return

    players_by_api_id = {}

    # 2. Rebuild the roster from current-season team contracts.
    for api_team_id, supabase_team_id in team_map.items():
        response = requests.get(
            "https://api.balldontlie.io/v1/contracts/teams",
            headers=headers,
            params={"team_id": api_team_id, "season": current_season},
            timeout=30,
        )

        if response.status_code != 200:
            print(f"Error fetching contracts for team {api_team_id}: {response.status_code} {response.text}")
            continue

        contracts = response.json().get('data', [])
        for contract in contracts:
            player = contract.get('player', {})
            if not player:
                continue

            players_by_api_id[str(player['id'])] = {
                "api_id": str(player['id']),
                "team_id": supabase_team_id,
                "name": f"{player['first_name']} {player['last_name']}",
                "position": player.get('position', ''),
                "is_active": True,
            }

    players_to_upsert = list(players_by_api_id.values())
    if not players_to_upsert:
        print(f"--- Finished! Total Active Players: 0 ---")
        return

    # 3. Remove old rows so only current players remain in the table.
    supabase.table("players").delete().neq("api_id", "").execute()

    supabase.table("players").upsert(players_to_upsert, on_conflict="api_id").execute()
    print(f"--- Finished! Total Active Players: {len(players_to_upsert)} ---")












if __name__ == "__main__":
    sync_nba_teams()
    sync_current_nba_players()
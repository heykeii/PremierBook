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
    # 2. Prefer using season stats to identify players who actually played this season.
    #    This avoids retired/former players who remain attached historically.
    stats_url = "https://api.balldontlie.io/v1/stats"
    cursor = None
    while True:
        params = [("per_page", 100), ("seasons[]", current_season)]
        if cursor:
            params.append(("cursor", cursor))

        resp = requests.get(stats_url, headers=headers, params=params, timeout=30)
        if resp.status_code == 401:
            print("Stats endpoint unauthorized for this API key. Falling back to players endpoint (less accurate).")
            break
        if resp.status_code != 200:
            print(f"Error fetching season stats: {resp.status_code} {resp.text}")
            break

        stats_data = resp.json().get('data', [])
        for s in stats_data:
            player = s.get('player') or {}
            if not player:
                continue
            pid = str(player.get('id'))
            team_id = str(player.get('team_id') or s.get('team', {}).get('id', ''))
            if team_id in team_map:
                players_by_api_id[pid] = {
                    "api_id": pid,
                    "team_id": team_map[team_id],
                    "name": f"{player.get('first_name','')} {player.get('last_name','')}",
                    "position": player.get('position',''),
                    "is_active": True,
                }

        cursor = resp.json().get('meta', {}).get('next_cursor')
        if not cursor:
            break

    # If stats were unavailable (401) fall back to players endpoint filtered by team_ids.
    if not players_by_api_id:
        print("Attempting fallback: players endpoint with team_ids filter.")
        players_url = "https://api.balldontlie.io/v1/players"
        cursor = None
        active_ids = list(team_map.keys())
        while True:
            params = [("per_page", 100)]
            params.extend(("team_ids[]", tid) for tid in active_ids)
            if cursor:
                params.append(("cursor", cursor))

            resp = requests.get(players_url, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"Error fetching players fallback: {resp.status_code} {resp.text}")
                break

            data = resp.json().get('data', [])
            for p in data:
                team_data = p.get('team', {})
                tid = str(team_data.get('id', p.get('team_id', '')))
                if tid in team_map:
                    pid = str(p.get('id'))
                    players_by_api_id[pid] = {
                        "api_id": pid,
                        "team_id": team_map[tid],
                        "name": f"{p.get('first_name','')} {p.get('last_name','')}",
                        "position": p.get('position',''),
                        "is_active": True,
                    }

            cursor = resp.json().get('meta', {}).get('next_cursor')
            if not cursor:
                break

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
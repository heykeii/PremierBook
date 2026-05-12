import os
import requests
from supabase import create_client, Client

# 1. Setup Credentials
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
rapid_api_key = os.environ.get("RAPID_API_KEY")
supabase: Client = create_client(url, key)

HEADERS = {
    "X-RapidAPI-Key": rapid_api_key,
    "X-RapidAPI-Host": "tank01-fantasy-stats.p.rapidapi.com"
}

def sync_nba_teams_tank01():
    print("--- Syncing NBA Teams via Tank01 ---")
    url = "https://tank01-fantasy-stats.p.rapidapi.com/getNBATeams"
    # We set schedules=false and rosters=false to keep the response light
    params = {"schedules": "false", "rosters": "false", "statsToGet": "averages"}
    
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"Team API Error: {response.status_code}")
        return

    teams_data = response.json().get('body', [])
    teams_to_upsert = []

    for t in teams_data:
        teams_to_upsert.append({
            "api_id": str(t['teamID']),
            "full_name": t['teamCity'] + " " + t['teamName'],
            "abbreviation": t['teamAbv'], # This is the "Glue"
            "logo_url": t.get('nbaComLogo1'),
            "conference_division": f"{t.get('conference', 'N/A')}/{t.get('division', 'N/A')}"
        })

    if teams_to_upsert:
        supabase.table("teams").upsert(teams_to_upsert, on_conflict="api_id").execute()
        print(f"Successfully synced {len(teams_to_upsert)} teams.")

def sync_nba_players_tank01():
    print("--- Syncing NBA Players via Tank01 ---")
    
    # 1. Fetch teams from Supabase to map abbreviations
    teams_query = supabase.table("teams").select("id, abbreviation").execute()
    team_map = {t['abbreviation']: t['id'] for t in teams_query.data}

    if not team_map:
        print("!!! ERROR: Teams table is empty. Sync teams first !!!")
        return

    # 2. Fetch the player list
    api_url = "https://tank01-fantasy-stats.p.rapidapi.com/getNBAPlayerList"
    response = requests.get(api_url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Player API Error: {response.status_code}")
        return

    all_players = response.json().get('body', [])
    players_to_upsert = []

    for p in all_players:
        team_abbr = p.get('team') 
        
        # Now the team_abbr from Tank01 will match the abbreviations in our DB!
        if team_abbr in team_map:
            players_to_upsert.append({
                "api_id": str(p['playerID']),
                "team_id": team_map[team_abbr],
                "name": p['longName'],
                "position": p.get('pos', 'N/A'),
                "is_active": True
            })

    # 3. Batch Upload
    if players_to_upsert:
        print(f"Uploading {len(players_to_upsert)} players...")
        for i in range(0, len(players_to_upsert), 100):
            batch = players_to_upsert[i:i+100]
            supabase.table("players").upsert(batch, on_conflict="api_id").execute()
        
    print(f"Finished! Total Active Players: {len(players_to_upsert)}")

if __name__ == "__main__":
    # ORDER MATTERS: Sync teams first, then players
    sync_nba_teams_tank01()
    sync_nba_players_tank01()
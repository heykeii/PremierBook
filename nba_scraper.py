import os
import requests
from supabase import create_client, Client

# 1. Setup Credentials
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
bdl_api_key = os.environ.get("BDL_API_KEY")
supabase: Client = create_client(url, key)

def sync_nba_teams():
    print("Syncing NBA Teams...")
    headers = {"Authorization": bdl_api_key}
    # This endpoint returns ALL NBA teams
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

def sync_nba_players():
    print("Syncing NBA Players (Optimized Batch Mode)...")
    headers = {"Authorization": bdl_api_key}
    
    # 1. CACHE TEAMS: Fetch all teams once to avoid querying the DB in a loop
    teams_query = supabase.table("teams").select("id, api_id").execute()
    # Create a dictionary for instant lookup: { "api_id": "internal_uuid" }
    team_map = {t['api_id']: t['id'] for t in teams_query.data}
    
    cursor = None
    while True:
        url = "https://api.balldontlie.io/v1/players?per_page=100"
        if cursor:
            url += f"&cursor={cursor}"
            
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            break

        data = response.json()
        players = data.get('data', [])
        if not players:
            break

        # 2. BATCHING: Prepare a list of players to send all at once
        players_to_upsert = []
        
        for p in players:
            api_team_id = str(p['team']['id'])
            
            # Look up the internal ID from our local dictionary (Zero network cost!)
            if api_team_id in team_map:
                players_to_upsert.append({
                    "api_id": str(p['id']),
                    "team_id": team_map[api_team_id],
                    "name": f"{p['first_name']} {p['last_name']}",
                    "position": p.get('position', 'N/A'),
                    "is_active": True,
                    "injury_status": "Healthy"
                })

        # 3. SINGLE DATABASE CALL: Send the whole batch (up to 100 players)
        if players_to_upsert:
            try:
                supabase.table("players").upsert(players_to_upsert, on_conflict="api_id").execute()
                print(f"Successfully upserted {len(players_to_upsert)} players.")
            except Exception as e:
                print(f"Error during batch upsert: {e}")

        cursor = data.get('meta', {}).get('next_cursor')
        if not cursor:
            break
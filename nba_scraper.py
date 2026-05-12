import requests
import os
from supabase import create_client

# Setup
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
rapid_api_key = os.environ.get("RAPID_API_KEY") 
supabase = create_client(url, key)

HEADERS = {
    "X-RapidAPI-Key": rapid_api_key,
    "X-RapidAPI-Host": "tank01-fantasy-stats.p.rapidapi.com"
}

def sync_nba_players_tank01():
    print("--- Syncing NBA Players via Tank01 ---")
    
    # 1. Fetch current teams to map IDs
    teams_query = supabase.table("teams").select("id, abbreviation").execute()
    # Map abbreviation (LAL, BOS) to your UUID
    team_map = {t['abbreviation']: t['id'] for t in teams_query.data}

    # 2. Get the full player list (This is 1 request for the whole league!)
    api_url = "https://tank01-fantasy-stats.p.rapidapi.com/getNBAPlayerList"
    response = requests.get(api_url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return

    all_players = response.json().get('body', [])
    players_to_upsert = []

    for p in all_players:
        team_abbr = p.get('team')
        
        # Tank01 gives you clean active/inactive status
        if team_abbr in team_map:
            players_to_upsert.append({
                "api_id": p['playerID'],
                "team_id": team_map[team_abbr],
                "name": p['longName'],
                "position": p.get('pos', 'N/A'),
                "is_active": True
            })

    # 3. Batch Upload
    if players_to_upsert:
        # Wipe old data if you want a clean roster
        supabase.table("players").delete().neq("api_id", "").execute()
        
        # Upload in chunks of 100
        for i in range(0, len(players_to_upsert), 100):
            batch = players_to_upsert[i:i+100]
            supabase.table("players").upsert(batch).execute()
        
    print(f"Finished! Total Active Players: {len(players_to_upsert)}")

if __name__ == "__main__":
    sync_nba_players_tank01()
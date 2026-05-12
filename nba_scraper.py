import os
import time
import random
from supabase import create_client, Client

# Official NBA API imports
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonallplayers

# 1. Setup Credentials
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Headers are REQUIRED for the official NBA API to avoid 403 Forbidden errors
HEADERS = {
    'Host': 'stats.nba.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.nba.com/',
    'Origin': 'https://www.nba.com',
    'Connection': 'keep-alive',
}

def sync_nba_teams():
    print("--- Starting Official NBA Teams Sync ---")
    # nba_api's static teams list is built into the library (no network call needed)
    nba_teams = teams.get_teams()
    
    teams_to_upsert = []
    for team in nba_teams:
        # Note: NBA teams don't have conference/division in the static list, 
        # but we can add them manually or leave as N/A for now.
        teams_to_upsert.append({
            "api_id": str(team['id']),
            "full_name": team['full_name'],
            "abbreviation": team['abbreviation'],
            "conference_division": "N/A" 
        })

    if teams_to_upsert:
        supabase.table("teams").upsert(teams_to_upsert, on_conflict="api_id").execute()
        print(f"Successfully synced {len(teams_to_upsert)} official NBA teams.")

def sync_current_nba_players():
    print("--- Syncing Official Active NBA Roster ---")
    
    # 1. Map the teams from Supabase
    teams_query = supabase.table("teams").select("id, api_id").execute()
    team_map = {t['api_id']: t['id'] for t in teams_query.data}

    if not team_map:
        print("No teams found. Please sync teams first.")
        return

    # 2. Fetch players with Retry Logic
    player_rows = None
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching player data (Attempt {attempt + 1})...")
            loader = commonallplayers.CommonAllPlayers(
                is_only_current_season=1, 
                league_id='00', 
                headers=HEADERS,
                timeout=120  # Increased timeout to 2 minutes
            )
            player_rows = loader.get_dict()['resultSets'][0]['rowSet']
            break # Success! Exit the retry loop
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                print(f"Waiting {wait}s before retrying...")
                time.sleep(wait)
            else:
                print("Max retries reached. Skipping player sync for now.")
                return

    # 3. Clean and Batch Upsert
    if player_rows:
        players_to_upsert = []
        for row in player_rows:
            api_pid = str(row[0])
            display_name = row[1]
            api_team_id = str(row[3])

            if api_team_id in team_map:
                players_to_upsert.append({
                    "api_id": api_pid,
                    "team_id": team_map[api_team_id],
                    "name": display_name,
                    "position": "N/A",
                    "is_active": True,
                })

        if players_to_upsert:
            print(f"Inserting {len(players_to_upsert)} active players...")
            # Using a simple upsert to avoid wiping data if the sync is partial
            for i in range(0, len(players_to_upsert), 100):
                batch = players_to_upsert[i:i+100]
                supabase.table("players").upsert(batch, on_conflict="api_id").execute()
            
            print("NBA Players Sync Complete.")

if __name__ == "__main__":
    sync_nba_teams()
    # Adding a small polite delay between big tasks
    time.sleep(random.uniform(1.5, 3.0))
    sync_current_nba_players()
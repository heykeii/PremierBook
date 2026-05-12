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
    print("Syncing NBA Players...")
    headers = {"Authorization": bdl_api_key}
    # Fetching players (paginated)
    response = requests.get("https://api.balldontlie.io/v1/players?per_page=100", headers=headers)
    
    if response.status_code != 200:
        print(f"Error fetching players: {response.status_code}")
        return

    players = response.json()['data']

    for p in players:
        # Link player to the team using the team's api_id
        team_id_query = supabase.table("teams").select("id").eq("api_id", str(p['team']['id'])).execute()
        
        if team_id_query.data:
            internal_team_id = team_id_query.data[0]['id']
            supabase.table("players").upsert({
                "api_id": str(p['id']),
                "team_id": internal_team_id,
                "name": f"{p['first_name']} {p['last_name']}",
                "position": p.get('position', 'N/A')
            }, on_conflict="api_id").execute()

if __name__ == "__main__":
    sync_nba_teams()
    sync_nba_players()
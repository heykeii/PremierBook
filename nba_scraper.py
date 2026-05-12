import os
import requests
from supabase import create_client, Client

#CREDENTIALS
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
bdl_api_key = os.environ.get("BDL_API_KEY")
supabase: Client = create_client(url,key)

#syncing nba teams
def sync_nba_teams():
    print("Syncing NBA teams...")
    headers = {"Authorization": bdl_api_key}
    #API Endpoints for getting NBA Teams
    response = requests.get("https://api.balldontlie.io/v1/teams", headers=headers)
    teams = response.json()['data']

    for team in teams:
        if team['league'] == 'NBA':
            supabase.table("teams").upsert({
                "api_id": str(team['id']),
                "full_name": team['full_name'],
                "abbreviation": team['abbreviation'],
                "conference_division": f"{team['conference']}/{team['division']}"
            }, on_conflict="api_id").execute()

def sync_nba_players():
    print("Syncing NBA Players...")
    headers = {"Authorization": bdl_api_key}
    # We fetch the first 100 players for the MVP
    response = requests.get("https://api.balldontlie.io/v1/players?per_page=100", headers=headers)
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
                "position": p['position']
            }, on_conflict="api_id").execute()

if __name__ == "__main__":
    sync_nba_teams()
    sync_nba_players()


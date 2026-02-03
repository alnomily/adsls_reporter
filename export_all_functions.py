import requests
import json

SUPABASE_URL = "https://aovyfdounbmrzyciesjt.supabase.co"
ANON_KEY = "sb_publishable_GW1_Bk_cB4ylMt3IZmM5Zw_13DqPrp8"

headers = {
    "apikey": ANON_KEY,
    "Authorization": f"Bearer {ANON_KEY}",
    "Content-Type": "application/json"
}

# Use Supabase's REST API for rpc (functions)
response = requests.get(
    f"{SUPABASE_URL}/rest/v1/rpc?select=*",
    headers=headers
)

if response.status_code == 200:
    functions = response.json()
    print(f"Found {len(functions)} functions via API")
    
    with open("supabase_functions_api.json", "w") as f:
        json.dump(functions, f, indent=2)
else:
    print(f"Error: {response.status_code}")
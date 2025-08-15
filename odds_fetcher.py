## schedule url is https://oddspedia.com/api/v1/getMatchList?excludeSpecialStatus=0&sortBy=default&perPageDefault=100&startDate=2025-08-11T04:00:00Z&endDate=2025-08-18T03:59:00Z&geoCode=US&status=all&sport=baseball&popularLeaguesOnly=0&category=usa&league=mlb&seasonId=125735&round=&page=1&perPage=100&language=us
## THIS IS BEFORE THE SUPPORT OF oddsgroupID1 or spread. 
import requests

BASE_URL = "https://test.tommyek600.workers.dev/"

response = requests.get(BASE_URL)

def decimal_to_american(decimal_odds):
    if decimal_odds >= 2:
        return round((decimal_odds - 1) * 100)
    else:
        return round(-100 / (decimal_odds - 1))

def fetch_odds():
    info = response.json()
    
    market_type = info['data']['inplay'][1].get("name", "unknown")
    print("Market Type:", market_type)
    
    for period in info['data']['inplay'][1].get("periods", []):
        for odd in period.get("odds", []):
            bookie_name = odd.get("bookie_name", "unknown")

            try:
                odds1_float = float(odd.get("o1", 0))
                odds2_float = float(odd.get("o2", 0))
            except ValueError:
                continue  # Skip if odds aren't valid numbers

            odds1_american = decimal_to_american(odds1_float)
            odds2_american = decimal_to_american(odds2_float)

            print(f"Bookie Name: {bookie_name}")
            print(f"Decimal Odds: {odds1_float}, {odds2_float}")
            print(f"American Odds: {odds1_american}, {odds2_american}")
            print()

if response.status_code == 200:
    fetch_odds()
else:
    print("Error Fetching. Status Code:", response.status_code)

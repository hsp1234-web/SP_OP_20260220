import requests
import json
import logging
import sys
from pathlib import Path

# Add project root to path to import modules if needed, but here we use raw requests
sys.path.append(str(Path(__file__).parent))

def fetch_real_data():
    url = "https://api.finmindtrade.com/api/v4/data"

    # Try TaiwanOptionOpenInterestLargeTraders with data_id
    params = {
        "dataset": "TaiwanOptionOpenInterestLargeTraders",
        "data_id": "TXO",
        "start_date": "2023-10-04",
        "end_date": "2023-10-04",
    }

    print(f"Fetching data from {url} with params: {params}...")
    try:
        response = requests.get(url, params=params)

        if response.status_code != 200:
             print(f"First attempt failed: {response.status_code} {response.text}")
             # Try without data_id if first fails, or maybe TaiwanStockPrice as fallback for POC
             params2 = {
                "dataset": "TaiwanStockPrice",
                "data_id": "2330",
                "start_date": "2023-10-04",
                "end_date": "2023-10-04"
             }
             print(f"Retrying with TaiwanStockPrice: {params2}")
             response = requests.get(url, params=params2)

        response.raise_for_status()
        data = response.json()

        if data.get("msg") == "success":
            output_path = Path("QuantDataPipeline/test_mock.json")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Successfully saved mock data to {output_path}")

            # Verify we have data
            if data.get("data"):
                print(f"Retrieved {len(data['data'])} records.")
            else:
                print("Warning: 'data' field is empty!")
        else:
            print(f"API Error: {data.get('msg')}")

    except Exception as e:
        print(f"Failed to fetch data: {e}")

if __name__ == "__main__":
    fetch_real_data()

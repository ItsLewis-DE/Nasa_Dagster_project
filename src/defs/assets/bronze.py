import dagster as dg
from dotenv import load_dotenv
import requests
import os
from  pathlib import Path
import json
import datetime
load_dotenv()

@dg.asset(group_name="bronze")
def fetch_data_from_API():
    """
        Extract data from API.Source from Nasa
    """
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params= {
        "start_date":((datetime.date.today() - datetime.timedelta(days=7))).isoformat(),
        "end_date":datetime.date.today().isoformat(),
        "api_key":os.getenv("API_KEY")
    }
    resp = requests.get(url,params=params)
    path = Path("src/defs/data/nasa_data.json")
    with open(path,"w") as f:
        json.dump(resp.json(),f,indent=2)
    return resp.json()

    

    

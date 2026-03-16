import json
import dagster as dg
import pandas as pd
@dg.asset(group_name="email")
def ingest_data_from_csv():
    """
    read response data from gg form
    """
    url = "https://docs.google.com/spreadsheets/d/1BcM-yUYbb8vDk-QkNX-xUVouV-Ag6s_ALg5A8pswvog/export?format=csv"
    df = pd.read_csv(url)
    path = "src/nasa_project/defs/data/request_email.json"
    df.to_json(path,indent=2)
    return path

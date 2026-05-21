import dagster as dg
import json
@dg.asset(
    deps =["ingest_data_from_csv"],
    group_name="email"
)
def transform_gg_form_data(ingest_data_from_csv):
    """
    tranfrom data from gg form
    """
    with open(ingest_data_from_csv) as f:
        data = json.load(f)
    email=list(data["Your email"].values())
    path = "src/defs/data/email.json"
    with open(path,"w") as f:
        json.dump(email,f,indent=2)

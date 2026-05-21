import dagster as dg
job_fetch_data = dg.define_asset_job(
    name = "job_fetch_data",
    selection=["fetch_data_from_API"]
)
job_send_email = dg.define_asset_job(
    name="job_send_email",
    selection=["database_into_csv","send_email"]
)
job_gg_form =dg.define_asset_job(
    name="job_gg_form",
    selection=["ingest_data_from_csv","transform_gg_form_data"]
)

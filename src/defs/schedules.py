from .jobs import job_fetch_data
from .jobs import job_gg_form
import dagster as dg

data_update_schedule = dg.ScheduleDefinition(
    job=job_fetch_data,
    cron_schedule="0 0 * * *"
)
data_update_gg_form = dg.ScheduleDefinition(
    job=job_gg_form,
    cron_schedule="* * * * *"
)

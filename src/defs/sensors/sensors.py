import os
import json
import dagster as dg
from src.defs.jobs.jobs import job_send_email
@dg.sensor(
    job = job_send_email
)
def email_request_sensor(context:dg.SensorEvaluationContext):
    """
    Sent email to customer when file asteroid.csv exist and when customer fill out gg form
    """
    state = json.loads(context.cursor) if context.cursor else {}
    last_line=state.get("email",0)
    path_email="src/defs/data/email.json"
    if not os.path.exists(path_email):
        return dg.SensorResult(cursor=context.cursor)
    with open(path_email) as f:
        emails = json.load(f)
    if last_line >=len(emails):
        return dg.SensorResult(cursor=context.cursor)
    email = emails[last_line]
    file_asteroids = "src/defs/data/asteroid.csv"
    if not os.path.exists(file_asteroids):
        return dg.SensorResult(cursor=context.cursor)

    run_request = dg.RunRequest(
        run_key=f"email_{last_line}",
        run_config={
            "ops": {
                "send_email": {
                    "config": {
                        "email": email
                    }
                }
            }
        }
    )

    new_state = {"email":last_line+1}
    return dg.SensorResult(
        run_requests=[run_request],
        cursor=json.dumps(new_state)
    )

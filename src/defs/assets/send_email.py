import dagster as dg
import smtplib
import json
import os
from dotenv import load_dotenv
from email.message import EmailMessage
load_dotenv()
class EmailRequestConfig(dg.Config):
    email:str

@dg.asset(
    deps = ["database_into_csv"],
    group_name="email"
)
def send_email(config:EmailRequestConfig,database_into_csv):
    """
    send email to customer when they fill out gg form
    """
    msg=EmailMessage()
    email=config.email
    msg["From"]=os.getenv("EMAIL_USER")
    msg["To"]=email
    msg["Subject"]="This is a new hazard asteroid!"
    with open(database_into_csv,"rb") as f:
        file_data = f.read()
    msg.add_attachment(
        file_data,
        maintype="application",
        subtype="octet-stream",
        filename=os.path.basename(database_into_csv)
    )
    with smtplib.SMTP(os.getenv("EMAIL_HOST"), int(os.getenv("EMAIL_PORT"))) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(
            os.getenv("EMAIL_USER"),
            os.getenv("EMAIL_PASSWORD")
        )
        server.send_message(msg)

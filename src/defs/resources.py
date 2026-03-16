from dagster_mysql import MySQLResource
import dagster as dg
from dotenv import load_dotenv
load_dotenv()
import os 
mysql_resource=MySQLResource(
    host=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT")),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE")
)
defs=dg.Definitions(
    resources= {
        "mysql":mysql_resource
    }
)

import dagster as dg
from dagster_mysql import MySQLResource
from src.defs.assets import silver_transform
import csv
import datetime
import os
@dg.asset(
    deps = [silver_transform.transform_data],
    required_resource_keys={"mysql"},
    group_name="gold"
)
def daily_asteroid(context):
    mysql = context.resources.mysql
    with mysql.get_connection() as conn:
        cursor =conn.cursor()
        query_create = """
            CREATE TABLE IF NOT EXISTS gold_daily_asteroid_stats (
                    date DATE,
                    asteroid_count INT,
                    hazardous_count INT,
                    average_velocity FLOAT,
                    diameter_max_km FLOAT,
                    diameter_min_km FLOAT
            )
        """
        cursor.execute(query_create)
        query_truncate = """
        TRUNCATE TABLE gold_daily_asteroid_stats
        """
        cursor.execute(query_truncate)
        query_insert = """
            INSERT INTO gold_daily_asteroid_stats(
                date,
                asteroid_count,
                hazardous_count,
                average_velocity,
                diameter_max_km,
                diameter_min_km
            )
            SELECT date,
                COUNT(id) as asteroid_count,
                SUM(
                CASE 
                    WHEN is_hazard = 'Hazard' THEN 1
                    ELSE 0 
                    END 
                    ) AS hazardous_count,
                AVG(velocity_km_h) as average_velocity,
                MAX(diameter_max_km) AS diameter_max_km,
                MIN(diameter_min_km) AS diameter_min_km
            FROM silver_nasa
            GROUP BY date
            ORDER BY date ASC
        """
        cursor.execute(query_insert)
        conn.commit()

@dg.asset(
    deps = [silver_transform.transform_data],
    required_resource_keys={"mysql"},
    group_name="gold"
)
def top_dangerous(context):
    mysql = context.resources.mysql
    with mysql.get_connection() as conn:
        cursor = conn.cursor()
        query_create = """
            CREATE TABLE IF NOT EXISTS gold_top_dangerous_asteroids(
                id BIGINT PRIMARY KEY,
                neo_id VARCHAR(50),
                name VARCHAR(50),
                diameter_min_km FLOAT,
                diameter_max_km FLOAT,
                close_approach_date DATE,
                distance_km FLOAT,
                danger_score FLOAT
            )
        """
        cursor.execute(query_create)
        query_truncate= """
        TRUNCATE TABLE gold_top_dangerous_asteroids;
        """
        cursor.execute(query_truncate)
        query_insert= """
        INSERT INTO gold_top_dangerous_asteroids(
            id,
            neo_id,
            name,
            diameter_min_km,
            diameter_max_km,
            close_approach_date,
            distance_km,
            danger_score
        )
            SELECT id,
                neo_id,
                name,
                diameter_min_km,
                diameter_max_km,
                close_approach_date,
                distance_km,
                danger_score
            FROM silver_nasa
            ORDER BY danger_score DESC,velocity_km_h DESC  
            LIMIT 10
        """
        cursor.execute(query_insert)
        conn.commit()
@dg.asset(
    required_resource_keys={"mysql"},
    group_name="gold"
)
def database_into_csv(context):
    mysql = context.resources.mysql
    with mysql.get_connection() as conn:
        cursor= conn.cursor()
        query=f"""
        SELECT id,
        name,
        danger_score,
        date
        FROM silver_nasa
        WHERE is_hazard = "Hazard" AND date='{datetime.date.today()}'
        """
        cursor.execute(query)
        rows=cursor.fetchall()
    file_path="src/defs/data/asteroid.csv"
    if not rows:
        if os.path.exists(file_path):
            os.remove(file_path)
        return None
    with open(file_path,"w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(["id","name","danger_score","date"])
        for row in rows:
            csv_writer.writerow(row)
    return file_path












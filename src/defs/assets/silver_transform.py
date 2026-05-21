import dagster as dg
import pandas as pd     
from dagster_mysql import MySQLResource
from src.defs.assets import silver_write_into_db

@dg.asset(
    deps = [silver_write_into_db.clean_data],
    required_resource_keys ={"mysql"},
    group_name="silver"
)
def transform_data(context):
    """
    insert data into silver table and transform data
    """
    mysql = context.resources.mysql
    with mysql.get_connection() as conn:
        cursor = conn.cursor()
        query_create = """
            CREATE TABLE IF NOT EXISTS silver_nasa (
                id BIGINT PRIMARY KEY,
                neo_id VARCHAR(50),
                name VARCHAR(50), 
                magnitude_km FLOAT,
                diameter_min_km FLOAT ,
                diameter_max_km FLOAT,
                is_hazard VARCHAR(50),
                close_approach_date DATE,
                close_approach_date_full DATETIME,
                velocity_km_h FLOAT,
                distance_km FLOAT,
                danger_score FLOAT,
                date DATE
            )

        """
        cursor.execute(query_create)
        query_truncate="""
        TRUNCATE TABLE silver_nasa
        """
        cursor.execute(query_truncate)
        query_insert = """
        INSERT INTO silver_nasa(
            id,
            neo_id,
            name, 
            magnitude_km,
            diameter_min_km,
            diameter_max_km,
            is_hazard,
            close_approach_date,
            close_approach_date_full,
            velocity_km_h,
            distance_km,
            danger_score,
            date
            )
            SELECT id,
                neo_id,
                TRIM(name) AS name, 
                absolute_magnitude_h AS magnitude_km,
                estimated_diameter_min_by_kilo AS  diameter_min_km,
                estimated_diameter_max_by_kilo AS diameter_max_km,
                CASE
                    WHEN is_hazard = '0' THEN 'Not hazard'
                    ELSE 'Hazard'
                    END AS is_hazard,
                close_approach_date,
                close_approach_date_full,
                COALESCE(velocity_kmh,0) AS velocity_km_h,
                distance_by_kilometers AS distance_km,
                danger_score,
                date
                FROM bronze_nasa
            """  
        cursor.execute(query_insert)
        conn.commit()

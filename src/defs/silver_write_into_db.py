import dagster as dg
import json
from datetime import datetime
from src.nasa_project.defs import bronze
@dg.asset (
    deps=[bronze.fetch_data_from_API],
    required_resource_keys={"mysql"},
    group_name="silver"
)
def clean_data(context,fetch_data_from_API):
    """
    Insert data into bronze table  and create a new column danger_score
    """
    row =[]
    data=fetch_data_from_API
    if "near_earth_objects" not in data:
        raise ValueError(f"Invalid API response: {data}")

    for date in data["near_earth_objects"]:
        for obj in data["near_earth_objects"][date]:

            diameter=obj["estimated_diameter"]["kilometers"]
            if not obj["close_approach_data"]: 
                continue
                
            approach=obj["close_approach_data"][0]
            velocity=approach["relative_velocity"]
            danger_score = float(diameter["estimated_diameter_max"])*float(velocity["kilometers_per_hour"])/float(approach["miss_distance"]["kilometers"])
            rows = (
                obj["id"],
                obj["neo_reference_id"],
                obj["name"],
                obj["absolute_magnitude_h"],
                float(diameter["estimated_diameter_min"]),
                float(diameter["estimated_diameter_max"]),
                obj["is_potentially_hazardous_asteroid"],
                approach["close_approach_date"],
                datetime.strptime(approach["close_approach_date_full"],"%Y-%b-%d %H:%M"),
                float(velocity["kilometers_per_hour"]),
                float(approach["miss_distance"]["kilometers"]),
                danger_score,
                date
            )
            row.append(rows)
            
    mysql=context.resources.mysql

    with mysql.get_connection() as conn:
        cursor=conn.cursor()
        query="""
            CREATE TABLE IF NOT EXISTS bronze_nasa (
                id  BIGINT PRIMARY KEY,
                neo_id VARCHAR(50),
                name VARCHAR(255),
                absolute_magnitude_h FLOAT,
                estimated_diameter_min_by_kilo FLOAT,
                estimated_diameter_max_by_kilo FLOAT,
                is_hazard VARCHAR(50),
                close_approach_date DATE,
                close_approach_date_full DATETIME,
                velocity_kmh FLOAT,
                distance_by_kilometers DOUBLE,
                danger_score FLOAT,
                date DATE
            )
            """
        cursor.execute(query)
        insert_query = """
        INSERT INTO bronze_nasa VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
        neo_id = VALUES(neo_id),
        name = VALUES(name),
        absolute_magnitude_h = VALUES(absolute_magnitude_h),
        estimated_diameter_min_by_kilo = VALUES(estimated_diameter_min_by_kilo),
        estimated_diameter_max_by_kilo = VALUES(estimated_diameter_max_by_kilo),
        is_hazard = VALUES(is_hazard),
        close_approach_date = VALUES(close_approach_date),
        close_approach_date_full = VALUES(close_approach_date_full),
        velocity_kmh = VALUES(velocity_kmh  ),
        distance_by_kilometers = VALUES(distance_by_kilometers),
        danger_score= VALUES(danger_score),
        date = VALUES(date)
        """
        cursor.executemany(insert_query, row)
        conn.commit()

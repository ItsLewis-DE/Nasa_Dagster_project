#NASA Asteroid Data Pipeline

A data pipeline that ingests Near-Earth Object data from NASA API, processes it using a Medallion architecture (Bronze -> Silver -> Gold), and detects hazardous asteroids using Dagster sensors.

---------------------------------------------------------------------------------------------------------------
Architecture:

NASA API
   
Dagster Ingestion Asset ( Ingest Automatically at 00:00 daily)
   
Bronze Layer (raw asteroid data)
   
Silver Layer (cleaned and structured data)
   
Gold Layer (analytics tables)

Dagster Sensor (hazard detection)

Email Alert ( Using GG Form to collect Email)

---------------------------------------------------------------------------------------------------------------
Tech Stack:
  - Python
  - Dagster (data orchestration)
  - MySQL (data warehouse)
  - Pandas / SQL (data transformation)
  - NASA Near Earth Object API
---------------------------------------------------------------------------------------------------------------
Data Pipeline Layers:

Bronze Layer:
Raw data ingested from NASA API

Silver Layer:
Clearned asteroid data with normalized schema and calculated danger score

Gold layer:
Aggregated analytics tables such as:
  - gold_daily_asteroid_stats
  - gold_top_dangerous_asteroids
----------------------------------------------------------------------------------------------------------------
Sensor Logic:

Hazard Detection Sensor
A Dagster sensor contunously monutors the dataset and trigger alearts when a hazardous asteroid is detected.

Conditions:
- is_hazard = True

  The sensor stores the last processed email last_line using Dagster cursor to avoid sending duplicate emails.

------------------------------------------------------------------------------------------------------------------

## Project Structure

```
src/
│
├── defs/
│   │
│   ├── data/                    
│   │   ├── email.json
│   │   ├── nasa_data.json
│   │   └── request_email.json
│   │
│   ├── bronze.py              
│   ├── ingest_gg_form.py        
│   │
│   ├── silver_transform.py     
│   ├── silver_write_into_db.py  
│   │
│   ├── transform_data_gg_form.py
│   │
│   ├── gold_layer.py           
│   │
│   ├── send_email.py            
│   │
│   ├── sensors.py             
│   ├── schedules.py         
│   ├── jobs.py                  
│   ├── resources.py              
│   │
│   └── definitions.py           
│
└── README.md
```

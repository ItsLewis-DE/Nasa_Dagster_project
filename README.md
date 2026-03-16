#NASA Asteroid Data Pipeline

A data pipeline that ingests Near-Earth Object data from NASA API, processes it using a Medallion architecture (Bronze -> Silver -> Gold), and detects hazardous asteroids using Dagster sensors.
---------------------------------------------------------------------------------------------------------------
NASA API
   ↓
Dagster Ingestion Asset ( Ingest Automatically at 00:00 daily)
   ↓
Bronze Layer (raw asteroid data)
   ↓
Silver Layer (cleaned and structured data)
   ↓
Gold Layer (analytics tables)
   ↓
Dagster Sensor (hazard detection)
   ↓
Email Alert ( Using GG Form to collect Email)
---------------------------------------------------------------------------------------------------------------
Tech Stack:
  - Python
  - Dagster (data orchestration)
  - MySQL (data warehouse)
  - Pandas / SQL (data transformation)
  - NASA Near Earth Object API
---------------------------------------------------------------------------------------------------------------
Bronze Layer:
Raw data ingested from NASA API

Silver Layer:
Clearned asteroid data with normalized schema and calculated danger score

Gold layer:
Aggregated analytics tables such as:
  - gold_daily_asteroid_stats
  - gold_top_dangerous_asteroids
----------------------------------------------------------------------------------------------------------------
Hazard Detection Sensor
A Dagster sensor contunously monutors the dataset and trigger alearts when a hazardous asteroid is detected.

Conditions:
- is_hazard = True

  The sensor stores the last processed email last_line using Dagster cursor to avoid sending duplicate emails.

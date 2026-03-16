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

src/
│
├── defs/
│   │
│   ├── data/                     # Local data storage
│   │   ├── email.json            # Email configuration
│   │   ├── nasa_data.json        # Raw data fetched from NASA API
│   │   └── request_email.json    # Email request payload
│   │
│   ├── bronze.py                 # Bronze layer: raw data ingestion
│   ├── ingest_gg_form.py         # Data ingestion from Google Form
│   │
│   ├── silver_transform.py       # Data cleaning and transformation
│   ├── silver_write_into_db.py   # Load cleaned data into database
│   │
│   ├── transform_data_gg_form.py # Transform Google Form data
│   │
│   ├── gold_layer.py             # Gold layer analytics tables
│   │
│   ├── send_email.py             # Email sending logic
│   │
│   ├── sensors.py                # Dagster sensors (hazard detection)
│   ├── schedules.py              # Pipeline scheduling
│   ├── jobs.py                   # Dagster jobs definitions
│   ├── resources.py              # External resources (MySQL, etc.)
│   │
│   └── definitions.py            # Dagster repository definitions
│
└── README.md

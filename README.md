
# ⛅️ Weather Data ETL Pipeline (Open-Meteo → PostgreSQL via Airflow)






## Overview

This project demonstrates a basic yet robust ETL (Extract, Transform, Load) pipeline for ingesting weather data from the [Open-Meteo API](https://open-meteo.com/) into a PostgreSQL database. The pipeline is orchestrated using **Apache Airflow** and containerized with **Docker**, all managed using **Astronomer**. It serves as a practical example of data engineering workflows and automation.
## 🛠️ Tech Stack

| Technology     | Description                                      |
|----------------|--------------------------------------------------|
| Python         | ETL scripting, API interaction, data transformation |
| Apache Airflow | Workflow orchestration and scheduling            |
| Docker         | Containerization of services                     |
| PostgreSQL     | Relational database to store weather data        |
| Astronomer     | Airflow deployment and environment management    |
| Open-Meteo API | Public weather data API                          |



## Architecture

## ⚙️ Approach / ETL Steps

The ETL pipeline is orchestrated using Apache Airflow and containerized via Docker (managed by Astronomer). Here's the approach:

---

### 1. **Extract Weather Data (API Connection)**
- Connect to the Open-Meteo API using Python `requests`.
- Pull JSON data for specified coordinates and time range.
- API key is not required.

---

### 2. **Transform Raw Data**
- Use `pandas` to normalize JSON into structured format.
- Clean timestamps, handle missing fields, and prepare the schema for loading.

---

### 3. **Load to PostgreSQL**
- Connect to a PostgreSQL database running inside Docker.
- Load transformed data using SQLAlchemy.

---

### 4. **Airflow DAG & Scheduling**
- A DAG in `dags/weather_etl_dag.py` automates the ETL flow.
- Scheduled to run daily (can be adjusted).
- Includes logging, retries, and task modularity.

---

### 5. **Establish Required Connections**

#### A. PostgreSQL Connection (Airflow)
- Set up a `postgres_default` connection in the Airflow UI or via CLI.
- Connection enables DAG to write to the PostgreSQL DB container.

#### B. Open-Meteo API (Handled in Code)
- The API does not require authentication.
- URL, params, and location coordinates are defined within the ETL script.

---

### 6. **Docker Configuration**
- PostgreSQL and Airflow run as Docker containers.
- Ensure PostgreSQL is defined in `docker-compose.override.yml` with appropriate environment variables:
  - `POSTGRES_DB`
  - `POSTGRES_USER`
  - `POSTGRES_PASSWORD`

---

### 7. **Run with Astronomer**
- Run in bash
```bash
astro dev start

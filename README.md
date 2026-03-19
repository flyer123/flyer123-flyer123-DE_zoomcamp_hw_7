# DE Zoomcamp HW 7 — Streaming

This repository contains the solutions for **Homework 7: Streaming Data Pipelines** from the [Data Engineering Zoomcamp 2026 cohort](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/07-streaming/homework.md).

We work with **NYC green taxi trip data** for October 2025 and implement streaming pipelines with **Kafka**, **Flink**, and **PostgreSQL**.

---

## Table of Contents

- [Overview](#overview)  
- [Architecture](#architecture)  
- [Tasks](#tasks)  
  - [Task 1–3: Raw Events to PostgreSQL](#task-1-3-raw-events-to-postgresql)  
  - [Task 4: Hourly Tip Aggregation](#task-4-hourly-tip-aggregation)  
  - [Task 5: Session Window Tip Aggregation](#task-5-session-window-tip-aggregation)  
- [Running the Pipelines](#running-the-pipelines)  
- [Files](#files)  
- [References](#references)  

---

## Overview

The goal of this homework is to implement a **streaming ETL pipeline** for NYC taxi trip data:

1. Produce events to Kafka from parquet datasets.  
2. Consume events in Flink and store in PostgreSQL.  
3. Perform aggregations using **tumbling windows** (hourly) and **session windows**.  

---

## Architecture
+----------------+ +---------+ +----------------+ +-------------+
| producer.py | ----> | Kafka | ----> | Flink Streaming| ----> | PostgreSQL |
| (Python) | | broker | | jobs | | tables |
+----------------+ +---------+ +----------------+ +-------------+
Flow:

1. producer.py / producer_kafka.py reads NYC green taxi parquet data
   and sends each trip event to Kafka topic green-trips.

2. Flink streaming jobs:

     - consumer_postgres.py (Task 1–3): write raw events to processed_events.

     - aggregation_job.py (Task 4): sum tips per hour (tumbling window) → processed_events_hourly.

     - aggregation_job_session.py (Task 5): sum tips per session window → processed_events_sessions.

3. PostgreSQL stores processed results for querying and analysis.

---

## Tasks

### Task 1–3: Raw Events to PostgreSQL

- **Goal:** Ingest raw taxi trip events into PostgreSQL.  
- **Producer:** `producer.py` (run in Jupyter)  
- **Consumer / Flink Job:** `consumer_postgres.py`  
- **PostgreSQL table schema:**

```sql
CREATE TABLE processed_events (
    PULocationID INT,
    DOLocationID INT,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    lpep_pickup_datetime TIMESTAMP(3),
    kafka_key STRING,
    PRIMARY KEY (lpep_pickup_datetime) NOT ENFORCED
);

Task 4: Hourly Tip Aggregation

 - Goal: Compute total tip_amount per 1-hour tumbling window.

 - Producer: producer.py

 - Flink Job: aggregation_job.py

 - PostgreSQL table schema:

 CREATE TABLE processed_events_hourly (
    window_start TIMESTAMP(3),
    total_tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start) NOT ENFORCED
);

Task 5: Session Window Tip Aggregation

 - Goal: Compute total tip_amount per session (5-minute gap).

 - Producer: producer_kafka.py

 - Flink Job: aggregation_job_session.py

 - PostgreSQL table schema:

 CREATE TABLE processed_events_sessions (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    total_tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start) NOT ENFORCED
);

Task 6: In Progress

 - Task 6 is not finished yet.

 - The producer run is handled in Jupyter; see logs in Docker Compose for reference.

 Running the Pipelines

  1. Start Kafka, Zookeeper, and PostgreSQL using Docker Compose.

  2. Run the producer (producer.py or producer_kafka.py) in Jupyter.

  3. Execute Flink jobs via Python scripts (consumer_postgres.py, aggregation_job.py, aggregation_job_session.py).

  4. Query the PostgreSQL tables to validate data ingestion and aggregation.

  | File                         | Description                                                      |
| ---------------------------- | ---------------------------------------------------------------- |
| `producer.py`                | Reads parquet data and sends events to Kafka (tasks 1–4).        |
| `producer_kafka.py`          | Variant producer for session aggregation (task 5).               |
| `consumer_postgres.py`       | Flink job: consumes events and writes to PostgreSQL (tasks 1–3). |
| `aggregation_job.py`         | Flink job: tumbling window aggregation (task 4).                 |
| `aggregation_job_session.py` | Flink job: session window aggregation (task 5).                  |

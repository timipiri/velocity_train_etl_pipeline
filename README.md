# velocity_train_etl_pipeline
This repository is for a data engineering project on train time table

TECH STACK SUMMARY:
Python + Requests: For API calls and initial data ingestion.

Kafka: For streaming real-time data and ensuring scalable, fault-tolerant message processing.

Great Expectations: For validating data dynamically to ensure data quality.

Airflow: To orchestrate the entire pipeline (scheduling, retries, alerts, etc.).

Azure + PostgreSQL: For storing validated data with high availability and fault tolerance.

HIGH LEVEL STEPS:
Install requirements
Import necessary Libraries
Logic for Data Ingestion
Logic for Data Transformation
Logic for Data Validation
Logic for Data Duplication
Airflow Dag for Pipeline Orchestration 



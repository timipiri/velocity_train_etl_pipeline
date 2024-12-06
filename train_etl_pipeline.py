# importing necessary libraries

import pandas as pd
import requests
import json
import time
from datetime import datetime
import pickle
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv 
import os
import logging
import great_expectations as ge
import kafka
from kafka import KafkaProducer
from great_expectations.validator.validator import Validator
from great_expectations.execution_engine.pandas_execution_engine import PandasExecutionEngine
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations import get_context
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations import get_context
import tempfile

# Data Ingestion Layer
def data_ingestion_layer(**kwargs):
    load_dotenv()

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    app_id = os.getenv('API_ID')
    app_key = os.getenv('API_KEY')
    station_code = 'WAT'
    url = f'https://transportapi.com/v3/uk/train/station/{station_code}/live.json'

    params = {
        'app_id': app_id,
        'app_key': app_key,
        'darwin': 'false',
        'train_status': 'passenger',
        'live': 'True',
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        response_json = response.json()
        kwargs['ti'].xcom_push(key='raw_response', value=response_json)
        producer.send('train_departures', {'data': response_json})
        print("Data successfully sent to Kafka topic: train_departures")
    else:
        print(f"Failed to fetch data: {response.status_code}")



# Data Transformation layer
def data_transformation_layer(**kwargs):
    response = kwargs['ti'].xcom_pull(key='raw_response')
    
    train_columns = [{'request_time': response['request_time'], 'station_name': response['station_name']}]
    train_columns_df = pd.DataFrame(train_columns).reset_index()

    train_departure_columns = []
    for column in response['departures']['all']:
        try:
            train_departure_columns.append({
                'mode': column['mode'],
                'train_uid': column['train_uid'],
                'origin_name': column['origin_name'],
                'operator_name': column['operator_name'],
                'platform': column['platform'],
                'destination_name': column['destination_name'],
                'aimed_departure_time': column['aimed_departure_time'],
                'expected_departure_time': column['expected_departure_time'],
                'best_departure_estimate_mins': column['best_departure_estimate_mins'],
                'aimed_arrival_time': column['aimed_arrival_time']
            })
        except KeyError as e:
            print(f"Missing data: {e}")

    train_departure_columns_df = pd.DataFrame(train_departure_columns).reset_index()

    merged_df = pd.merge(train_columns_df, train_departure_columns_df, on='index', how='outer')
    merged_df.rename(columns={'request_time': 'request_date_time'}, inplace=True)
    merged_df.fillna('unknown', inplace=True)
    merged_df.drop(columns=['index'], inplace=True)

    csv_path = '/tmp/train_schedule.csv'
    merged_df.to_csv(csv_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_csv_path', value=csv_path)



# Data Validation Layer
def data_validation_layer(**kwargs):
    csv_path = kwargs['ti'].xcom_pull(key='transformed_csv_path')
    train_schedule_df = pd.read_csv(csv_path)

    context = get_context()
    suite = ExpectationSuite("train_schedule_suite")
    execution_engine = PandasExecutionEngine()

    batch = Batch(data=train_schedule_df)
    validator = Validator(execution_engine=execution_engine, batches=[batch], expectation_suite=suite)

    columns = train_schedule_df.columns
    for column in columns:
        validator.expect_column_values_to_not_be_null(column=column)

    validation_results = validator.validate()
    print(f"Validation results: {validation_results}")



# Data duplication layer
def data_duplication_layer(**kwargs):
    import dotenv
    import os
    import psycopg2

    # Reload .env file to ensure updated environment variables
    dotenv.load_dotenv(override=True)

    # Load transformed CSV path from XCom
    csv_path = kwargs['ti'].xcom_pull(key='transformed_csv_path')

    # Connection details
    local_conn_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    azure_conn_params = {
        'dbname': os.getenv('AZURE_DB_NAME'),
        'user': os.getenv('AZURE_DB_USER'),
        'password': os.getenv('AZURE_DB_PASSWORD'),
        'host': os.getenv('AZURE_DB_HOST'),
        'port': os.getenv('AZURE_DB_PORT')
    }

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS train_schedule (
        id SERIAL PRIMARY KEY,
        request_date_time VARCHAR(100),
        station_name VARCHAR(100),
        mode VARCHAR(100),
        train_uid VARCHAR(100),
        origin_name VARCHAR(100),
        operator_name VARCHAR(100),
        platform VARCHAR(100),
        destination_name VARCHAR(100),
        aimed_departure_time VARCHAR(100),
        expected_departure_time VARCHAR(100),
        best_departure_estimate_mins VARCHAR(100),
        aimed_arrival_time VARCHAR(100)
    );
    """

    def close_previous_connection(conn):
        if conn:
            try:
                conn.close()
                print("Previous connection closed successfully.")
            except Exception as e:
                print(f"Error closing connection: {e}")

    # Insert data into both databases
    for conn_params in [local_conn_params, azure_conn_params]:
        db_name = conn_params['dbname']
        conn = None

        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()

            # Create table if it doesn't exist
            print(f"Creating table in database: {db_name}")
            cursor.execute(CREATE_TABLE_SQL)
            conn.commit()

            # Insert data from CSV (excluding the id column)
            print(f"Inserting data into database: {db_name}")
            with open(csv_path, 'r') as f:
                cursor.copy_expert(
                    """
                    COPY train_schedule (
                        request_date_time, station_name, mode, train_uid, origin_name, operator_name, 
                        platform, destination_name, aimed_departure_time, expected_departure_time, 
                        best_departure_estimate_mins, aimed_arrival_time
                    )
                    FROM STDIN WITH CSV HEADER
                    """,
                    f
                )

            conn.commit()
            print(f"Data successfully inserted into {db_name}")

        except psycopg2.OperationalError as e:
            print(f"Database connection error for {db_name}: {e}")

        except Exception as e:
            print(f"Error inserting data into {db_name}: {e}")
            conn.rollback()

        finally:
            if cursor:
                cursor.close()
            close_previous_connection(conn)
            print(f"Connection to {db_name} closed.")

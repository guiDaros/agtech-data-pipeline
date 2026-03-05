from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='agtech_ingestion_pipeline',
    default_args=default_args,
    description='Orchestrates IoT and Weather data ingestion.',
    schedule_interval='0 */3 * * *',
    catchup=False,
    tags=['agtech', 'ingestion', 'production'],
) as dag:

    ingest_weather = BashOperator(
        task_id='ingest_weather_api',
        bash_command='python /opt/airflow/pipelines/ingestion/load_weather_api.py',
    )

    ingest_sensors = BashOperator(
        task_id='ingest_sensor_data',
        bash_command='python /opt/airflow/pipelines/ingestion/load_sensor_data.py',
    )

    [ingest_weather, ingest_sensors]
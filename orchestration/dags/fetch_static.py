from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import zipfile
import io
import os

BKK_STATIC_URL = "https://futar.bkk.hu/gtfs/budapest_gtfs.zip"
BRONZE_STATIC_DIR = "/opt/airflow/data/lakehouse/bronze/static"

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id='bkk_static_ingestion_nightly',
    default_args=default_args,
    description='Downloads daily BKK GTFS-Static schedules',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bkk', 'bronze'],
)
def bkk_static_pipeline():
    @task
    def download_and_extract_gtfs():
        url = "https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"
        
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Failed to download. Status: {response.status_code}")

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall("/opt/airflow/workspace/data/static")
        
        print("Static GTFS data extracted successfully!")

    download_and_extract_gtfs()

bkk_static_pipeline()

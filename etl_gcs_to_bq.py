from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
import os
from google.cloud import storage
os.environ["GOOGLE_CLOUD_PROJECT"] = "red-tide-376509"



@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    absolute_path = "/c/Users/user/Downloads/DataTalks ZoomCamp/data-engineering-zoomcamp/week_2_workflow_orchestration/02_gcp/flows/02_gcp/data/yellow/yellow_tripdata_2022-01.parquet"
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    gcs_path = f"{dataset_file}.parquet"
    csv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    local_path = "../data/"
    try:
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        gcp_cloud_storage_bucket_block.get_directory(
            from_path=f"data/{color}/{gcs_path}", local_path=local_path
        )
    except Exception as e:
        print(f"Failed to extract data from GCS: {e}. Trying to extract from URL.")
        try:
            os.system(f"wget {csv_url}")
            os.system(f"gunzip {dataset_file}.csv.gz")
            df = pd.read_csv(f"{dataset_file}.csv")
            df.to_parquet(f"{local_path}/{gcs_path}")
        except Exception as e:
            print(f"Failed to extract data from URL: {e}")
            try:
                local_files = [f for f in os.listdir(local_path) if os.path.isfile(f"{local_path}/{f}")]
                if gcs_path in local_files:
                    return Path(f"{local_path}/{gcs_path}")
                elif os.path.isfile(absolute_path):
                    return Path(absolute_path)
                else:
                    raise Exception(f"No existing local data found.")
            except Exception as e:
                raise Exception(f"Failed to use existing local data: {e}")
    return Path(f"{local_path}/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise ValueError(f"Reading parquet file failed: {e}")

    print(f"pre: missing passenger count: {df['passenger_count'].isnull().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isnull().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("krono-creds")

    # Create session and set retries
    session = requests.Session()
    retries = HTTPAdapter(max_retries=3)
    session.mount("https://", retries)

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="red-tide-376509",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
        session=session
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
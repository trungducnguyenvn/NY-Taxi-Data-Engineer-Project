import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


@task
def download_data(url, dataset_file):
    """Download data from url"""
    # Get response content from url
    response = requests.get(url).content

    # Convert bytes to PyArrow table
    table = pq.read_table(pa.BufferReader(response))

    # Convert PyArrow table to Pandas DataFrame
    df = table.to_pandas()

    return df



@task()
def load_to_gcs(data: pd.DataFrame , color: str, dataset_file: str) -> None:
    """Upload data to GCS"""
    gcs_block = GcsBucket.load("ny-taxi-gcs")
    to_path = f"{color}/{dataset_file}.parquet"
    gcs_block.upload_from_dataframe(data, to_path=to_path, serialization_format="parquet")
    return



@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet'

    data = download_data(dataset_url, dataset_file)
    load_to_gcs(data, color, dataset_file)



if __name__ == "__main__":
    color = "green"
    year = 2021
    for month in range(1, 4):
        etl_web_to_gcs(color, year, month)



import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    gcs_path = f"{color}/{dataset_file}.parquet"
    gcs_block = GcsBucket.load("ny-taxi-gcs")
    gcs_block.download_object_to_path(from_path= gcs_path, to_path= f"../data/{color}/{dataset_file}.parquet")
    return Path(f"../data/{color}/{dataset_file}.parquet")


@task()
def cleaning(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    # ehail_fee is always null
    print(f"pre: missing ehail_fee: {df['ehail_fee'].isna().sum()}")
    df.drop(columns=["ehail_fee"], inplace=True)

    # Check for negative values in fare_amount and total_amount and tip_amount and improvement_surcharge and extra and tolls_amount
    print(f"pre: negative fares: {(df['fare_amount'] < 0).sum()}")
    print(f"pre: negative total_amount: {(df['total_amount'] < 0).sum()}")
    print(f"pre: negative tip_amount: {(df['tip_amount'] < 0).sum()}")
    print(f"pre: negative improvement_surcharge: {(df['improvement_surcharge'] < 0).sum()}")
    print(f"pre: negative extra: {(df['extra'] < 0).sum()}")    
    print(f"pre: negative tolls_amount: {(df['tolls_amount'] < 0).sum()}")

    # Remove negative fares and total_amount and tip_amount and improvement_surcharge and extra and tolls_amount
    df = df[df["total_amount"] >= 0]

    # After removing negative fares and total_amount and tip_amount and improvement_surcharge and extra and tolls_amount
    print(f"post: negative fares: {(df['fare_amount'] < 0).sum()}")
    print(f"post: negative total_amount: {(df['total_amount'] < 0).sum()}")
    print(f"post: negative tip_amount: {(df['tip_amount'] < 0).sum()}")
    print(f"post: negative improvement_surcharge: {(df['improvement_surcharge'] < 0).sum()}")
    print(f"post: negative extra: {(df['extra'] < 0).sum()}")
    print(f"post: negative tolls_amount: {(df['tolls_amount'] < 0).sum()}")

    # payment_type
    print(f"pre: missing payment_type: {df['payment_type'].isna().sum()}")
    df["payment_type"].fillna(0, inplace=True)

    # passenger_count
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("bq-data-editor-creds")

    # Write a DataFrame to a Google BigQuery table
    df.to_gbq(
        destination_table="ny_taxi.rides",
        project_id="nyc-project-raw",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append",
    )


@flow(name="etl_gcs_to_bq")
def etl_gcs_to_bq(color: str, year: int, month: int):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = cleaning(path)
    write_bq(df)


if __name__ == "__main__":
    color = "green"
    year = 2021
    for month in [1, 2, 3 ]:
        etl_gcs_to_bq(color, year, month)
    
    

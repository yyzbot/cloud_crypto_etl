
import json
import boto3
import pandas as pd
from io import StringIO  # For temporary storage in memory
from datetime import datetime


def load_raw_data_from_s3(bucket_name, file_name):
    """
    Load raw cryptocurrency data from S3 bucket.

    Args:
        bucket_name (str) & file_name (str): 
        Extracted from file_path passed from event output of extraction state

    """
    s3 = boto3.client("s3")
    # Get file object from S3
    raw_file_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    # Load only actual content of the file
    raw_data = json.loads(raw_file_obj["Body"].read().decode("utf-8"))
    return raw_data


def transform_data(raw_data, fetch_time):
    """
    Transform raw cryptocurrency data, including cleaning and addition of new metrics.

    Args:
        fetch_time (str): Timestamp passed from event output of extraction state

    """
    # Convert raw data into a Pandas DataFrame
    df = pd.DataFrame(raw_data)

    # Drop unnecessary columns
    fields_to_drop = ["image", "roi"]
    df = df.drop(columns=fields_to_drop, errors="ignore")

    # Drop rows with missing values
    df = df.dropna()

    # Drop duplicate rows
    df = df.drop_duplicates()

    # Convert `fetch_time` to ISO 8601 format for better loading into database
    fetch_time_datetime = pd.to_datetime(fetch_time, format="%Y-%m-%dT%H-%M-%SZ", errors="coerce")
    fetch_time_iso = fetch_time_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    df["fetch_time"] = fetch_time_iso 

    # Convert other timestamp columns to ISO 8601 format
    timestamp_columns = ["ath_date", "atl_date", "last_updated"]
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Calculate dominance percentage for each cryptocurrency
    total_market_cap = df["market_cap"].sum()
    df["dominance_percentage"] = (df["market_cap"] / total_market_cap) * 100

    # Calculate volume percentage for each cryptocurrency
    total_volume = df["total_volume"].sum()
    df["volume_percentage"] = (df["total_volume"] / total_volume) * 100

    return df


def store_processed_data_to_s3_csv(processed_df, bucket_name, raw_file_name):
    """
    Convert processed data to CSV format and store it in the S3 bucket.

    """
    # Extract timestamp from the raw file name
    timestamp = raw_file_name.replace("raw_crypto_data_", "").replace(".json", "")

    # Define the name and path for the processed CSV file
    processed_file_name = f"processed_crypto_data_{timestamp}.csv"
    processed_file_path = f"s3://{bucket_name}/{processed_file_name}"

    # Convert DataFrame to CSV and store temporarily in memory
    csv_buffer = StringIO()
    processed_df.to_csv(csv_buffer, index=False)

    # Save CSV to S3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key=processed_file_name,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
    return processed_file_path


def lambda_handler(event, context):
    """
    Lambda handler.

    """
    # Extract raw file path, fetch_time, and bucket name from the event data passed from extraction state
    raw_file_path = event["file_path"]
    fetch_time = event["fetch_time"]
    bucket_name = raw_file_path.split("/")[2]
    raw_file_name = raw_file_path.split("/")[-1]

    # Run the script
    raw_data = load_raw_data_from_s3(bucket_name, raw_file_name)

    processed_df = transform_data(raw_data, fetch_time)

    processed_file_path = store_processed_data_to_s3_csv(processed_df, bucket_name, raw_file_name)

    # Extract processed file name from path
    processed_file_name = processed_file_path.split("/")[-1]

    # Return the bucket name and processed file name as event data for load state
    return {
        "status": "success",
        "bucket_name": bucket_name,
        "file_name": processed_file_name
    }

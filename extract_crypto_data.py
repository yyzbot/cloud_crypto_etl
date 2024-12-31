
import os  # Import the os module to access environment variables
import requests
import boto3  # Import the Boto3 library to interact with AWS services
import json
from datetime import datetime, timezone


def fetch_crypto_data():
    """
    Fetch cryptocurrency market data at the time point of request 
    for the top 500 coins using the CoinGecko API.

    Returns:
        results (list): Market data
    """
    # Get the CoinGecko API key from the environment variables
    api_key = os.getenv("COINGECKO_API_KEY")
    if not api_key:
        raise ValueError("API key not found. Please set the COINGECKO_API_KEY environment variable.")

    # Define the URL, headers, and parameters for the API request
    url = "https://api.coingecko.com/api/v3/coins/markets"
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": api_key,
    }
    params = {
        "vs_currency": "usd",              # Target currency for market data
        "order": "market_cap_desc",        # Sort by descending market cap
        "per_page": 250,                   # Set coins fetched per page
        "price_change_percentage": "24h",  # Set price change percentage timeframe
    }

    results = []

    for page in range(1, 3):  # Fetch data for pages 1 and 2
        params["page"] = page
        response = requests.get(url, headers=headers, params=params)
        results.extend(response.json())  # Append each dictionary element in the json to the results list

    return results


def store_raw_data_to_s3(data, bucket_name, fetch_time):
    """
    Store raw cryptocurrency market data in an S3 bucket.

    Args:
        data (list): Data to be stored (list of dictionaries)
        bucket_name (str): The name of the S3 bucket to upload data to
        fetch_time (str): Timestamp of script start
    """
    # Define the file name with the provided fetch_time
    file_name = f"raw_crypto_data_{fetch_time}.json"
    # Define the file path in S3
    file_path = f"s3://{bucket_name}/{file_name}"

    # Convert the data back to JSON format with indentation for readability
    raw_data_json = json.dumps(data, indent=2)

    # Create an S3 client object
    s3 = boto3.client("s3")

    # Upload the data to the S3 bucket
    s3.put_object(
        Bucket=bucket_name,  # Name of the S3 bucket
        Key=file_name,  # Name of the file to be uploaded
        Body=raw_data_json,  # Data to be uploaded
        ContentType="application/json",  # Set the content type of the data for S3 to properly handle it
    )

    # Return the S3 file path
    return file_path


def lambda_handler(event, context):
    """
    AWS Lambda handler function that triggers the script.

    Args:
        event (dict): Lambda event data (not used in this script)
        context (object): Lambda runtime information (not used in this script)

    Returns:
        dict: Status, S3 file path, and fetch_time as event data for use in transformation state
    """
    # Generate the fetch_time UTC timestamp 
    fetch_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')

    # Define the bucket name
    bucket_name = "cm--raw-data"

    # Run the script
    crypto_data = fetch_crypto_data()

    file_path = store_raw_data_to_s3(crypto_data, bucket_name, fetch_time)

    # Return the status, file path, and fetch_time
    return {
        "status": "success",
        "file_path": file_path,
        "fetch_time": fetch_time
    }

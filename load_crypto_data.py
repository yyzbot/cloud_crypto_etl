
import os
import snowflake.connector  # Import Snowflake Connector to interact with Snowflake

def load_processed_data_to_snowflake(bucket_name, file_name):
    """
    Loads processed cryptocurrency data from S3 to Snowflake.

    """
    # Retrieve Snowflake credentials from environment variables
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
    snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")
    snowflake_stage = os.getenv("SNOWFLAKE_STAGE")

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
    )

    # Create a cursor to execute Snowflake SQL commands
    cursor = conn.cursor()

    # Execute COPY INTO command to bulk-load CSV data from external stage into crypto_data table
    copy_query = f"""
    COPY INTO CRYPTO_ETL_DB.PUBLIC.CRYPTO_DATA
    FROM @crypto_stage/{file_name}
    FILE_FORMAT = CSV_FORMAT
    ON_ERROR = 'CONTINUE';
    """
    cursor.execute(copy_query)
    print(f"Data from '{file_name}' successfully loaded into 'CRYPTO_DATA' table.")

    # Close cursor and connection
    cursor.close()
    conn.close()

    return f"Data from '{file_name}' successfully loaded into Snowflake."

def lambda_handler(event, context):
    """
    Lambda handler
    """
    # Extract S3 bucket and file name from the event
    bucket_name = event["bucket_name"]
    file_name = event["file_name"]

    result_message = load_processed_data_to_snowflake(bucket_name, file_name)

    return {"status": "success", "message": result_message}

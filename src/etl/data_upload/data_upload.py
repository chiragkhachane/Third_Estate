import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve Snowflake configuration from environment variables
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')

# Define the directory paths and corresponding schemas
DATA_DIRECTORIES = {
    "raw": os.getenv('RAW_FILE_PATH'),
    "stage": os.getenv('STAGE_FILE_PATH'),
    "prod": os.getenv('PROD_FILE_PATH')
}

# Step 1: Establish a connection to Snowflake
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE
)
print("Successfully connected to Snowflake.")

def run_data_upload_pipeline():
    try:
        for schema, folder_path in DATA_DIRECTORIES.items():
            # Step 1: Ensure the schema exists; create if it doesn't
            print(f"Checking if schema '{schema}' exists...")
            create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{schema};"
            conn.cursor().execute(create_schema_query)
            print(f"Schema '{schema}' is ready.")

            # Set the schema for the upcoming table creation and data loading
            conn.cursor().execute(f"USE SCHEMA {schema};")

            # Step 2: Process each CSV file in the folder
            for filename in os.listdir(folder_path):
                if filename.endswith(".csv"):
                    table_name = os.path.splitext(filename)[0]  # Use the filename (without extension) as the table name
                    csv_file_path = os.path.join(folder_path, filename)

                    # Load the CSV file to get column names
                    print(f"Loading CSV file from {csv_file_path} for table {table_name}...")
                    df = pd.read_csv(csv_file_path)

                    # Define table schema with quoted column names and VARCHAR data type
                    column_names = [f'"{col.replace(" ", "_")}" VARCHAR' for col in df.columns]
                    table_schema = ", ".join(column_names)

                    # Step 3: Create the table if it doesn't exist
                    print(f"Creating table '{table_name}' in schema '{schema}' if it doesn't exist...")
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema});"
                    conn.cursor().execute(create_table_query)
                    print(f"Table '{table_name}' is ready.")

                    # Step 4: Upload file to Snowflake table stage
                    print(f"Uploading file {csv_file_path} to Snowflake stage for table {table_name}...")
                    put_command = f"PUT 'file://{csv_file_path}' @%{table_name} AUTO_COMPRESS=TRUE;"
                    conn.cursor().execute(put_command)
                    print(f"File {csv_file_path} uploaded successfully to stage.")

                    # Step 5: Load data into Snowflake table
                    print(f"Loading data from {csv_file_path} into table '{table_name}'...")
                    copy_into_query = f"""
                    COPY INTO {table_name}
                    FROM @%{table_name}/{filename}
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1)
                    ON_ERROR = 'CONTINUE';
                    """
                    conn.cursor().execute(copy_into_query)
                    print(f"Data successfully loaded into '{table_name}' from {csv_file_path}.")

    finally:
        # Close the connection
        print("Closing the connection to Snowflake...")
        conn.close()
        print("Connection closed.")

# bq_connector.py

import os
import sys
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

# --- Helper function to clean environment variables ---
def clean_env_var(var_value):
    """Removes leading/trailing whitespace, quotes, and inline comments."""
    if var_value:
        cleaned = var_value.strip()
        if cleaned.startswith('"') and cleaned.endswith('"'):
            cleaned = cleaned[1:-1]
        elif cleaned.startswith("'") and cleaned.endswith("'"):
            cleaned = cleaned[1:-1]
        cleaned = cleaned.split('#')[0].strip()
        return cleaned
    return None

def fetch_latest_job_data():
    """
    Connects to BigQuery using environment variables, executes a predefined
    query to fetch the latest job data (joining staging and augmented tables),
    and returns the results as a Pandas DataFrame along with the ingestion date.

    Returns:
        tuple: (pd.DataFrame, str or None):
                 - The resulting DataFrame. Empty if no data or error.
                 - The latest ingestion date string ('YYYY-MM-DD') if successful,
                   otherwise None.
    """
    print("--- Initializing BigQuery Connection ---")
    load_dotenv() # Load environment variables from .env

    # --- Load and Validate Credentials ---
    credentials_info = {
        "type": os.getenv("TYPE"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": os.getenv("PRIVATE_KEY", "").replace('\\n', '\n'),
        "client_email": os.getenv("CLIENT_EMAIL"),
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI"),
        "token_uri": os.getenv("TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
        "universe_domain": os.getenv("UNIVERSE_DOMAIN")
    }
    required_creds = ["type", "project_id", "private_key_id", "private_key", "client_email", "client_id"]
    missing_creds = [key for key in required_creds if not credentials_info.get(key)]
    if missing_creds:
        print(f"Error: Missing required credential environment variables: {', '.join(missing_creds)}")
        return pd.DataFrame(), None # Return empty DataFrame and None date

    # --- Load and Validate Configuration ---
    gcp_project_id = clean_env_var(os.getenv("GCP_PROJECT_ID"))
    linkedin_bq_project_id = clean_env_var(os.getenv("LINKEDIN_BQ_PROJECT_ID"))
    linkedin_bq_dataset_id = clean_env_var(os.getenv("LINKEDIN_BQ_DATASET_ID")) # Clean if needed
    linkedin_bq_table_id = clean_env_var(os.getenv("LINKEDIN_BQ_TABLE_ID"))     # Clean if needed
    linkedin_bq_augmented_table_id = clean_env_var(os.getenv("LINKEDIN_BQ_AUGMENTED_TABLE_ID")) # Clean if needed

    required_config = {
        "GCP_PROJECT_ID": gcp_project_id,
        "LINKEDIN_BQ_PROJECT_ID": linkedin_bq_project_id,
        "LINKEDIN_BQ_DATASET_ID": linkedin_bq_dataset_id,
        "LINKEDIN_BQ_TABLE_ID": linkedin_bq_table_id,
        "LINKEDIN_BQ_AUGMENTED_TABLE_ID": linkedin_bq_augmented_table_id,
    }
    missing_config = [name for name, value in required_config.items() if not value]
    if missing_config:
        print(f"Error: Missing required config environment variables: {', '.join(missing_config)}")
        return pd.DataFrame(), None

    # --- Create Credentials and BigQuery Client ---
    try:
        print("Attempting to create credentials...")
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        print("Credentials created.")

        print(f"Attempting to connect to BigQuery project: \"{gcp_project_id}\"...")
        client = bigquery.Client(credentials=credentials, project=gcp_project_id)
        print("BigQuery client created successfully.")

    except Exception as e:
        print(f"Error creating credentials or BigQuery client: {e}")
        return pd.DataFrame(), None

    # --- Construct and Execute Query ---
    staging_table_full_id = f"`{linkedin_bq_project_id}.{linkedin_bq_dataset_id}.{linkedin_bq_table_id}`"
    augmented_table_full_id = f"`{linkedin_bq_project_id}.{linkedin_bq_dataset_id}.{linkedin_bq_augmented_table_id}`"

    sql_query = f"""
    WITH LatestIngestion AS (
        SELECT MAX(ingestiondate) AS max_ingestion_date
        FROM {staging_table_full_id}
    )
    SELECT
        t1.*,
        -- Select all columns from augmented except the join key to avoid duplication
        t2.* EXCEPT (job_id)
    FROM
        {staging_table_full_id} AS t1
    INNER JOIN
        {augmented_table_full_id} AS t2 ON t1.job_id = t2.job_id
    CROSS JOIN
        LatestIngestion
    WHERE
        t1.ingestiondate = LatestIngestion.max_ingestion_date;
    """

    print("\n--- Executing BigQuery Query ---")
    print(sql_query) # Log the query being run

    df = pd.DataFrame() # Initialize empty DataFrame
    latest_date_str = None

    try:
        query_job = client.query(sql_query)
        print("Query submitted. Waiting for results...")
        results = query_job.result() # Waits for job completion
        print("Query completed.")

        df = results.to_dataframe() # Convert results to DataFrame

        if df.empty:
            print("Query returned no results for the latest ingestion date.")
            # Attempt to get the latest date even if no join results
            try:
                 date_query = f"SELECT FORMAT_DATE('%Y-%m-%d', MAX(ingestiondate)) FROM {staging_table_full_id}"
                 date_job = client.query(date_query)
                 latest_date_str = list(date_job.result())[0][0] # Get the single date value
                 print(f"Latest ingestion date found: {latest_date_str}")
            except Exception as date_e:
                 print(f"Could not determine latest ingestion date: {date_e}")
                 latest_date_str = None # Ensure it's None if lookup fails
        else:
            print(f"Loaded {len(df)} rows into DataFrame.")
            # Extract the date from the DataFrame (all rows should have the same)
            if 'ingestionDate' in df.columns:
                 # Convert to desired string format 'YYYY-MM-DD'
                 latest_date_obj = pd.to_datetime(df['ingestionDate'].iloc[0])
                 latest_date_str = latest_date_obj.strftime('%Y-%m-%d')
                 print(f"Latest ingestion date from data: {latest_date_str}")
            else:
                 print("Warning: 'ingestionDate' column not found in results.")


    except Exception as e:
        print(f"\nError executing BigQuery query or processing results: {e}")
        # Optional: Log more details from query_job.errors if needed
        if 'query_job' in locals() and hasattr(query_job, 'errors') and query_job.errors:
            print("Query Job Errors:")
            for error in query_job.errors:
                print(f"  - {error.get('message', 'N/A')} (Reason: {error.get('reason', 'N/A')})")
        # Return empty DataFrame and None date on error
        return pd.DataFrame(), None

    print("--- Data Fetch Complete ---")
    return df, latest_date_str
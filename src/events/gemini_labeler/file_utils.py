# src/file_utils.py
import os
import re
import json
import pandas as pd
from tqdm import tqdm # For logging within functions if needed
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging # Use logging for better error messages
from urllib.parse import urlparse, parse_qs
import re
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Import necessary config
# import config # Use relative import

def create_output_directory(dir_path: str):
    """Creates the output directory if it doesn't exist."""
    try:
        os.makedirs(dir_path, exist_ok=True)
        print(f"Ensured output directory exists: '{dir_path}'")
    except OSError as e:
        # Handle potential errors like permission issues
        print(f"Error creating output directory '{dir_path}': {e}")
        raise # Re-raise the exception to be handled by the caller

def read_parquet_safe(file_path: str) -> pd.DataFrame | None:
    """Reads a Parquet file with basic error handling."""
    try:
        df = pd.read_parquet(file_path)
        print(f"Successfully read {len(df)} rows from '{file_path}'.")
        return df
    except FileNotFoundError:
        print(f"Error: Parquet file not found at '{file_path}'.")
        return None
    except Exception as e:
        print(f"Error reading Parquet file '{file_path}': {e}")
        return None

def save_json(data: dict, filepath: str):
    """Saves a dictionary to a JSON file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        # Optional: print success message if needed, but might be too verbose in a loop
        # print(f"Successfully saved data to '{filepath}'")
    except IOError as e:
        # Use tqdm.write for consistency if called within the loop's context
        tqdm.write(f"  Error: Failed to write output file {os.path.basename(filepath)}: {e}")
        raise # Re-raise to be caught by main loop's error handling
    except Exception as e:
        tqdm.write(f"  Error: An unexpected error occurred while saving {os.path.basename(filepath)}: {e}")
        raise # Re-raise

# --- NEW BigQuery Helper Functions ---
def get_credentials():
    """
    Create and return service account credentials from environment variables.
    Handles both file path and individual env var methods.
    """
    try:
        # Check if credentials file path is provided
        creds_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_file and os.path.exists(creds_file):
            logging.info(f"Using credentials from file: {creds_file}")
            # Ensure correct scope if needed, although BQ/GCS often infer defaults
            # scopes = ["https://www.googleapis.com/auth/cloud-platform"]
            # return service_account.Credentials.from_service_account_file(creds_file, scopes=scopes)
            return service_account.Credentials.from_service_account_file(creds_file)

        # Extract service account details from environment variables
        credentials_dict = {
            "type": os.getenv("TYPE", "service_account"), # Default type if missing
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            # Ensure correct newline replacement for private keys from env vars
            "private_key": os.getenv("PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": os.getenv("TOKEN_URI", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL", "https://www.googleapis.com/oauth2/v1/certs"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
            # Add universe_domain if needed for your environment, otherwise omit
            # "universe_domain": os.getenv("UNIVERSE_DOMAIN", "googleapis.com")
        }
        # Attempt to get universe_domain if set
        universe_domain = os.getenv("UNIVERSE_DOMAIN")
        if universe_domain:
            credentials_dict["universe_domain"] = universe_domain

        # Validate required credential fields extracted from env vars
        required_fields = ["project_id", "private_key", "client_email"]
        missing_fields = [field for field in required_fields if not credentials_dict.get(field)]

        if missing_fields:
            # Log details but raise a clearer error if no creds method worked
            logging.error(f"Missing required credential environment variables: {', '.join(missing_fields)}")
            raise ValueError(
                "Could not create credentials. Ensure GOOGLE_APPLICATION_CREDENTIALS file exists "
                f"OR all required env vars ({', '.join(required_fields)}) are set."
            )

        logging.info(f"Using credentials constructed from environment variables for project: {credentials_dict['project_id']}")
        # Ensure correct scope if needed
        # scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        # return service_account.Credentials.from_service_account_info(credentials_dict, scopes=scopes)
        return service_account.Credentials.from_service_account_info(credentials_dict)

    except Exception as e:
        logging.error(f"Failed to get or create credentials: {e}", exc_info=True)
        raise

# --- BigQuery Client using the above credentials ---
def get_bigquery_client():
    """
    Create and return an authenticated BigQuery client using credentials
    obtained via get_credentials().
    """
    try:
        # Get credentials using your defined method
        credentials = get_credentials()

        # Determine the project ID for the client.
        # Prioritize BQ specific, then general GCP, then the one from creds dict.
        project_id = os.getenv("BQ_PROJECT_ID") or os.getenv("GCP_PROJECT_ID") or credentials.project_id

        if not project_id:
             raise ValueError("Could not determine BigQuery project ID. Set BQ_PROJECT_ID, GCP_PROJECT_ID, or ensure PROJECT_ID is in credentials.")

        logging.info(f"Creating BigQuery client for project: {project_id} using provided service account credentials.")

        # Create and return the BigQuery client, explicitly passing the credentials
        return bigquery.Client(project=project_id, credentials=credentials)

    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}", exc_info=True)
        raise


def get_distinct_job_ids_from_bq(client: bigquery.Client, table_full_id: str, id_column: str) -> set:
    """
    Queries a BigQuery table to get a set of distinct job IDs (or links).
    Returns an empty set if the table doesn't exist or on error.
    """
    query = f"SELECT DISTINCT `{id_column}` FROM `{table_full_id}` WHERE `{id_column}` IS NOT NULL"
    logging.info(f"Querying distinct IDs from: {table_full_id}, column: {id_column}")
    try:
        query_job = client.query(query)
        results = query_job.result() # Waits for the job to complete
        # Use a set comprehension for efficiency
        ids = {row[id_column] for row in results}
        logging.info(f"Found {len(ids)} distinct IDs in {table_full_id}")
        return ids
    except NotFound:
        logging.warning(f"Table not found: {table_full_id}. Returning empty set.")
        return set()
    except Exception as e:
        logging.error(f"Error querying distinct IDs from {table_full_id}: {e}")
        # Depending on policy, you might want to return empty set or raise
        return set() # Return empty set to potentially allow processing if source exists

def get_jobs_to_process_from_bq(client: bigquery.Client, table_full_id: str, id_column: str, description_column: str, ids_to_fetch: set) -> pd.DataFrame | None:
    """
    Fetches rows from a BigQuery table for specific job IDs (or links).
    Returns a Pandas DataFrame or None on error.
    """
    if not ids_to_fetch:
        logging.info("No new job IDs provided to fetch.")
        return pd.DataFrame(columns=[id_column, description_column]) # Return empty DataFrame

    # Format IDs for the IN clause (handle strings correctly)
    formatted_ids = ", ".join(f"'{str(id_val)}'" for id_val in ids_to_fetch) # Ensure quoting for strings

    query = f"""
        SELECT
            `{id_column}`,
            `{description_column}`
        FROM
            `{table_full_id}`
        WHERE
            `{id_column}` IN ({formatted_ids})
    """
    logging.info(f"Querying {len(ids_to_fetch)} job details from: {table_full_id}")
    # Be cautious: Very large IN clauses can hit query limits. Consider batching if needed.
    query_job = client.query(query)
    df = query_job.to_dataframe()
    logging.info(f"Successfully fetched {len(df)} rows to process.")
    # Basic validation
    if id_column not in df.columns or description_column not in df.columns:
            logging.error(f"Query result missing required columns: '{id_column}', '{description_column}'.")
            return None
    return df
    
def upload_processed_data_to_bq(client: bigquery.Client, table_full_id: str, processed_data: dict, job_link: str, job_link_column_name: str) -> bool:
    """
    Uploads a single row of processed job data to the specified BigQuery table.

    Args:
        client: Authenticated google.cloud.bigquery.Client instance.
        table_full_id: The full ID of the target BigQuery table (e.g., "project.dataset.table").
        processed_data: A dictionary containing the structured data extracted by the Gemini processor.
                        The keys should match the top-level column names in the BQ table schema
                        (e.g., 'job_summary', 'company_information', etc.).
        job_link: The original job link used as the identifier.
        job_link_column_name: The name of the column in the target BQ table
                              where the job_link should be stored. This is crucial
                              for the differential read logic to work correctly.

    Returns:
        True if the insertion was successful (or reported as such by the API), False otherwise.
    """
    # **IMPORTANT**: Ensure your target BQ table (`linkedin_augmented_staging`)
    # includes a column to store the job link identifier. We assume this column's
    # name is passed via `job_link_column_name`.
    # Add the job link identifier to the data dictionary.
    row_to_insert = processed_data.copy() # Avoid modifying the original dict passed in
    row_to_insert[job_link_column_name] = job_link

    # BigQuery expects a list of rows for insertion.
    rows_to_insert = [row_to_insert]

    try:
        # Use insert_rows_json which handles dicts directly
        errors = client.insert_rows_json(table_full_id, rows_to_insert)

        if not errors:
            logging.info(f"Successfully inserted data for job link '{job_link}' into {table_full_id}")
            return True
        else:
            # Log errors returned by the BigQuery API for the failed rows
            logging.error(f"Encountered BigQuery errors while inserting data for job link '{job_link}' into {table_full_id}:")
            for error_detail in errors:
                logging.error(f" - Row index {error_detail['index']}: Errors: {error_detail['errors']}")
            # Check if the error is related to schema mismatch
            # (This is a basic check, specific error messages provide more detail)
            if any('schema' in str(err).lower() for err in error_detail['errors']):
                 logging.error("Potential schema mismatch detected. Verify the structure of 'processed_data' matches the BQ table schema.")
            return False # Indicate failure

    except NotFound:
        logging.error(f"Target table '{table_full_id}' not found. Cannot insert data.")
        return False
    except Exception as e:
        # Catch other potential exceptions (network issues, auth problems, etc.)
        logging.error(f"Failed to insert data for job link '{job_link}' into {table_full_id} due to an unexpected error: {e}", exc_info=True)
        return False
    
def ensure_schema_compliance(data: dict, template: dict) -> dict:
    """
    Recursively ensures the data dictionary conforms to the template structure.
    Adds missing keys from the template with default values (None, [], or nested dicts).

    Args:
        data: The dictionary potentially missing keys (e.g., from LLM).
        template: The dictionary representing the desired schema structure.

    Returns:
        The data dictionary, modified in-place, guaranteed to have keys from the template.
    """
    if not isinstance(data, dict):
        # If the data for a struct field is not a dict (e.g., None or something else),
        # we should probably replace it with a compliant structure.
        # Or, decide if this case indicates an upstream error.
        # For now, let's create a new dict based on the template for this level.
        logging.warning(f"Data expected to be a dict but found {type(data)}. Rebuilding from template.")
        data = {} # Start fresh for this level

    for key, template_value in template.items():
        if key not in data:
            # Key is missing in the data
            if isinstance(template_value, dict):
                # If template expects a nested struct, add an empty dict
                data[key] = {}
                # Recursively ensure the newly added dict is also compliant
                ensure_schema_compliance(data[key], template_value)
                logging.debug(f"Added missing nested key '{key}' with empty struct.")
            elif isinstance(template_value, list):
                 # If template expects an array, add an empty list
                data[key] = []
                logging.debug(f"Added missing key '{key}' with default empty list [].")
            else:
                # Otherwise (basic type like STRING, INT, BOOL), add None
                data[key] = None
                logging.debug(f"Added missing key '{key}' with default value None.")
        elif isinstance(template_value, dict) and isinstance(data.get(key), dict):
            # Key exists and both template and data expect/have a struct, recurse
            ensure_schema_compliance(data[key], template_value)
        # Optional: Handle cases where data[key] exists but is the wrong type
        # (e.g., data[key] is a string but template_value is a dict).
        # This adds complexity but might be needed depending on LLM variability.
        # For now, we primarily address missing keys.

    return data # Return the modified dictionary
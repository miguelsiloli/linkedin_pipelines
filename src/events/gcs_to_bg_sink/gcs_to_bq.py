import os
import logging
import json
import tempfile
import re
from datetime import date, datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import io
import time

from dotenv import load_dotenv

# --- Logging Setup (Optional but Recommended) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Authentication Setup (Using your provided functions) ---
def get_credentials():
    """
    Create and return service account credentials from environment variables.
    """
    try:
        # Check if credentials file path is provided
        creds_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_file and os.path.exists(creds_file):
            logging.info(f"Using credentials from file: {creds_file}")
            return service_account.Credentials.from_service_account_file(creds_file)

        # Extract service account details from environment variables
        credentials_dict = {
            "type": os.getenv("TYPE"),
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            # Ensure private_key exists before replacing newlines
            "private_key": os.getenv("PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"),
            "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
            "universe_domain": os.getenv("UNIVERSE_DOMAIN")
        }

        # Validate required credential fields
        required_fields = ["project_id", "private_key", "client_email"]
        # Filter out None or empty string values before checking missing fields
        valid_creds = {k: v for k, v in credentials_dict.items() if v}
        missing_fields = [field for field in required_fields if field not in valid_creds]

        if missing_fields:
             # If creds_file wasn't found either, *then* it's an error
             if not (creds_file and os.path.exists(creds_file)):
                raise ValueError(f"Missing required credential environment variables: {', '.join(missing_fields)}")
             else:
                 # This case should ideally not be reached due to the initial check,
                 # but added for robustness. It means file exists but env vars are missing.
                 # The file credentials should have already been returned.
                 logging.warning("Credential env vars missing, but proceeding with file credentials.")
                 # This part is technically unreachable if the first 'if creds_file' succeeds.

        logging.info("Using credentials from environment variables.")
        # Use valid_creds which filters out None/empty optional fields
        return service_account.Credentials.from_service_account_info(valid_creds)

    except Exception as e:
        logging.error(f"Failed to create credentials: {e}", exc_info=True)
        raise

def get_storage_client():
    """
    Create and return an authenticated GCS storage client.
    """
    try:
        # Get credentials
        credentials = get_credentials()

        # Determine project ID
        project_id = os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID")
        if not project_id:
            # Attempt to get project_id from credentials if available
             project_id = credentials.project_id if hasattr(credentials, 'project_id') else None
        if not project_id:
            raise ValueError("Could not determine project ID for Storage client. Set GCP_PROJECT_ID or PROJECT_ID env var, or ensure credentials contain it.")

        logging.info(f"Creating storage client for project: {project_id}")
        # Create and return the storage client with the credentials
        return storage.Client(project=project_id, credentials=credentials)

    except Exception as e:
        logging.error(f"Failed to create storage client: {e}", exc_info=True)
        raise

def get_bigquery_client():
    """
    Create and return an authenticated BigQuery client.
    """
    try:
        # Get credentials
        credentials = get_credentials()

        # Determine project ID (prioritize LinkedIn specific, then general GCP, then default)
        project_id = os.getenv("LINKEDIN_BQ_PROJECT_ID") or os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID")
        if not project_id:
             # Attempt to get project_id from credentials if available
             project_id = credentials.project_id if hasattr(credentials, 'project_id') else None
        if not project_id:
             raise ValueError("Could not determine project ID for BigQuery client. Set LINKEDIN_BQ_PROJECT_ID, GCP_PROJECT_ID, or PROJECT_ID env var, or ensure credentials contain it.")

        logging.info(f"Creating BigQuery client for project: {project_id}")
        # Create and return the BigQuery client with the credentials
        return bigquery.Client(project=project_id, credentials=credentials)

    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}", exc_info=True)
        raise

# --- GCS Functions ---

def find_latest_file_in_gcs_by_name(
    bucket_name: str,
    prefix: str,
    file_name_prefix: str = "linkedin_scrap_",
    file_name_suffix: str = ".parquet"
) -> Optional[str]:
    """
    Finds the most recent file in a GCS prefix based on filename sorting,
    expecting a 'YYYY-MM-DD' date format before the suffix.

    Args:
        bucket_name: The name of the GCS bucket.
        prefix: The prefix (subfolder path) within the bucket.
        file_name_prefix: The starting part of the filename to match.
        file_name_suffix: The ending part (extension) of the filename.

    Returns:
        The full GCS path (gs://...) of the latest file found by filename sort,
        or None if no matching files are found or an error occurs.
    """
    if not bucket_name or not prefix:
        logging.error("GCS_BUCKET_NAME and GCS_LINKEDIN_SUBFOLDER_PATH environment variables must be set.")
        return None

    if not prefix.endswith('/'):
        prefix_with_slash = prefix + '/'
    else:
        prefix_with_slash = prefix

    full_gcs_prefix = f"gs://{bucket_name}/{prefix_with_slash}"
    logging.info(f"Searching for latest file by name matching '{file_name_prefix}*{file_name_suffix}' in: {full_gcs_prefix}")

    try:
        # Uses the authentication helper function
        storage_client = get_storage_client()
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix_with_slash)

        matching_files = []
        for blob in blobs:
            file_name = os.path.basename(blob.name)
            # Ensure it's not just the prefix "folder" itself
            if blob.name == prefix_with_slash and blob.size == 0:
                 logging.debug(f"Ignoring prefix directory object: {blob.name}")
                 continue
            if file_name.startswith(file_name_prefix) and file_name.endswith(file_name_suffix):
                matching_files.append(blob.name) # Store the full blob name (path)
                logging.debug(f"Found potential file: {blob.name}")

        if not matching_files:
            logging.warning(f"No files matching pattern '{file_name_prefix}*{file_name_suffix}' found in {full_gcs_prefix}")
            return None

        # Sort filenames alphabetically (YYYY-MM-DD ensures latest is last)
        matching_files.sort()

        latest_blob_name = matching_files[-1]
        latest_file_gcs_path = f"gs://{bucket_name}/{latest_blob_name}"

        logging.info(f"Latest file found (by filename sort): {latest_file_gcs_path}")
        return latest_file_gcs_path

    except NotFound:
        logging.error(f"Bucket '{bucket_name}' not found or prefix '{prefix_with_slash}' does not exist.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while accessing GCS: {e}", exc_info=True)
        return None

def extract_date_from_filename(
    filename: str,
    pattern: str = r"linkedin_scrap_(\d{4}-\d{2}-\d{2})\.parquet"
) -> Optional[date]:
    """
    Extracts the date from a filename using a regex pattern.

    Args:
        filename: The filename (e.g., 'linkedin_jobs_data_2025-04-30.parquet').
        pattern: The regex pattern with a capturing group for the YYYY-MM-DD date.

    Returns:
        A datetime.date object if the date is found, otherwise None.
    """
    # Use os.path.basename to handle full paths correctly
    base_filename = os.path.basename(filename)
    match = re.search(pattern, base_filename)
    if match:
        date_str = match.group(1)
        try:
            extracted_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            logging.info(f"Extracted ingestion date {extracted_date} from filename '{base_filename}'")
            return extracted_date
        except ValueError:
            logging.error(f"Could not parse date string '{date_str}' found in filename '{base_filename}'")
            return None
    else:
        logging.warning(f"Could not find date pattern in filename '{base_filename}' using regex '{pattern}'")
        return None

def read_parquet_from_gcs(file_path: str) -> pd.DataFrame:
    """
    Read a Parquet file from GCS using the Google Cloud Storage client.
    """
    logging.info(f"Reading Parquet file from GCS: {file_path}")
    if not file_path.startswith('gs://'):
        raise ValueError(f"Invalid GCS path: {file_path}. Must start with 'gs://'")
    path_parts = file_path[5:].split('/', 1)
    if len(path_parts) != 2:
        raise ValueError(f"Invalid GCS path format: {file_path}")
    bucket_name, blob_name = path_parts
    try:
        # Uses the authentication helper function
        storage_client = get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Use download_as_bytes and pandas read_parquet directly from memory
        # This avoids potential issues with temp files on some systems (e.g. Cloud Run)
        logging.info(f"Downloading {blob_name} into memory.")
        file_bytes = blob.download_as_bytes()
        logging.info(f"Reading Parquet data from memory ({len(file_bytes)} bytes).")
        df = pd.read_parquet(io.BytesIO(file_bytes))

        # --- Alternative using tempfile (your original method) ---
        # with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as temp_file:
        #     logging.info(f"Downloading {blob_name} to temporary file {temp_file.name}")
        #     blob.download_to_filename(temp_file.name)
        #     # No need to flush/fsync before read_parquet usually
        #     logging.info(f"Reading Parquet data from {temp_file.name}")
        #     df = pd.read_parquet(temp_file.name)
        # --- End Alternative ---

        logging.info(f"Successfully read Parquet file: {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Failed to read Parquet file from GCS: {file_path} - {e}", exc_info=True)
        raise Exception(f"Failed to read Parquet file from GCS: {str(e)}") from e


# --- Data Processing ---

def process_linkedin_job_data(df: pd.DataFrame, ingestion_date: date) -> pd.DataFrame:
    """
    Transforms the raw LinkedIn job DataFrame to match the BigQuery schema
    and adds the ingestion date.

    Args:
        df: The DataFrame containing raw data from the Parquet file.
        ingestion_date: The date extracted from the filename.

    Returns:
        A DataFrame with the transformed data, ready for BigQuery insertion.
    """
    logging.info(f"Starting transformation for {len(df)} raw LinkedIn job records.")
    transformed_df = pd.DataFrame()

    try:
        # Define target columns based on BQ schema
        final_columns = [
            'job_id', 'job_title', 'company_name', 'location', 'employment_type',
            'experience_level', 'workplace_type', 'applicant_count', 'reposted_info',
            'skills_summary', 'application_type', 'job_description', 'job_link',
            'company_logo_url', 'source_file', 'ingestionDate'
        ]

        # Copy existing columns, handling potential missing ones
        for col in final_columns:
            if col == 'ingestionDate':
                transformed_df[col] = ingestion_date # Assign the fixed ingestion date
            elif col in df.columns:
                # Convert to string and handle potential NA values appropriately
                transformed_df[col] = df[col].astype(str).fillna(pd.NA)
            else:
                logging.warning(f"Source DataFrame missing column '{col}'. It will be added with NA values.")
                transformed_df[col] = pd.NA # Add missing columns as NA

        # Ensure correct data types where necessary (especially primary key and date)
        transformed_df['job_id'] = transformed_df['job_id'].astype(str)
        # Convert ingestionDate to datetime objects suitable for BQ DATE type
        # The BQ client library handles the conversion from pandas datetime/date to BQ DATE
        transformed_df['ingestionDate'] = pd.to_datetime(transformed_df['ingestionDate'])

        # Check for null job_ids which are part of the primary key
        null_job_ids = transformed_df['job_id'].isnull().sum()
        if null_job_ids > 0:
             logging.warning(f"Found {null_job_ids} rows with null job_id. These might cause issues if job_id is NOT NULL in BigQuery.")
             # Optional handling: drop them, fill with a placeholder, etc.
             # transformed_df = transformed_df.dropna(subset=['job_id'])

        # Reorder columns to match the final list for clarity
        transformed_df = transformed_df[final_columns]

        logging.info(f"Successfully transformed LinkedIn data. Resulting shape: {transformed_df.shape}")
        # logging.debug(f"Transformed DataFrame dtypes:\n{transformed_df.dtypes}")

    except KeyError as e:
        logging.error(f"Missing expected column in source DataFrame during processing: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"Error during LinkedIn data transformation: {e}", exc_info=True)
        raise

    return transformed_df

# --- BigQuery Functions ---
def check_existing_keys_in_bigquery(df: pd.DataFrame) -> List[tuple]:
    """
    Check which (job_id, ingestionDate) pairs already exist in the BigQuery table.
    
    Args:
        df: DataFrame containing the keys to check (must have 'job_id' and 'ingestionDate' columns)
        
    Returns:
        List of (job_id, ingestionDate) tuples that already exist in BigQuery
    """
    # Get BigQuery configuration from environment variables
    bq_project_id = os.getenv("LINKEDIN_BQ_PROJECT_ID") or get_bigquery_client().project
    bq_dataset_id = os.getenv("LINKEDIN_BQ_DATASET_ID")
    bq_table_id = os.getenv("LINKEDIN_BQ_TABLE_ID")
    
    # Validate required configuration
    if not all([bq_project_id, bq_dataset_id, bq_table_id]):
        raise ValueError("Missing required BigQuery config (LINKEDIN_BQ_PROJECT_ID/Client Project, LINKEDIN_BQ_DATASET_ID, LINKEDIN_BQ_TABLE_ID)")
    
    # Construct full table ID
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    
    try:
        # Ensure 'ingestionDate' is in datetime format before extracting date part
        if not pd.api.types.is_datetime64_any_dtype(df['ingestionDate']):
            df['ingestionDate'] = pd.to_datetime(df['ingestionDate'], errors='coerce')
            
        # Get unique (job_id, ingestionDate) pairs from DataFrame
        key_pairs = df[['job_id', 'ingestionDate']].dropna().drop_duplicates()
        
        # Convert ingestionDate column to date objects consistently
        key_pairs['ingestionDate'] = key_pairs['ingestionDate'].dt.date
        
        if len(key_pairs) == 0:
            logging.info("No valid key pairs (job_id, ingestionDate) to check in BigQuery after processing.")
            return []
        
        # Create temporary table ID for the query
        temp_dataset = f"{bq_dataset_id}"
        temp_table_name = f"temp_lookup"
        temp_table_id = f"{bq_project_id}.{temp_dataset}.{temp_table_name}"
        
        # Get BigQuery client
        bq_client = get_bigquery_client()
        
        # Ensure temp dataset exists
        try:
            bq_client.get_dataset(f"{bq_project_id}.{temp_dataset}")
        except Exception:
            dataset = bigquery.Dataset(f"{bq_project_id}.{temp_dataset}")
            bq_client.create_dataset(dataset, exists_ok=True)
        
        # Define explicit schema to match the target table
        schema = [
            bigquery.SchemaField("job_id", "STRING"),
            bigquery.SchemaField("ingestionDate", "DATE")  # Use DATE type to match BigQuery
        ]
        
        # Create the table with explicit schema
        table = bigquery.Table(temp_table_id, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        
        # Upload the lookup DataFrame to BigQuery
        logging.info(f"Creating temporary lookup table with {len(key_pairs)} key pairs")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        load_job = bq_client.load_table_from_dataframe(
            key_pairs, temp_table_id, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        
        # Execute query to find matching keys
        query = f"""
        SELECT t.job_id, t.ingestionDate
        FROM `{full_table_id}` t
        INNER JOIN `{temp_table_id}` l
        ON t.job_id = l.job_id
        AND t.ingestionDate = l.ingestionDate
        """
        
        # Run the query
        logging.info("Checking for existing key pairs in BigQuery")
        query_job = bq_client.query(query)
        
        # Collect results 
        existing_keys = []
        for row in query_job:
            # Convert back to the format needed for comparison
            existing_keys.append((row.job_id, row.ingestionDate))

        logging.info(f"Found {len(existing_keys)} existing key pairs in BigQuery")

        # Attempt to clean up the temporary table
        try:
            bq_client.delete_table(temp_table_id)
            logging.info(f"Deleted temporary table {temp_table_id}")
        except Exception as e:
            logging.warning(f"Could not delete temporary table: {e}")
        
        return existing_keys
        
    except Exception as e:
        logging.error(f"Error checking existing keys in BigQuery: {e}", exc_info=True)
        # Raising is safer to prevent accidental duplicate loads if the check fails
        raise

def filter_out_duplicates(df: pd.DataFrame, existing_keys: List[Tuple[str, date]]) -> pd.DataFrame:
    """
    Filters out rows from the DataFrame whose (job_id, ingestionDate) pair
    already exists in BigQuery.

    Args:
        df: DataFrame containing the data to filter (must have 'job_id', 'ingestionDate').
        existing_keys: List of (job_id, ingestionDate<date object>) tuples that exist in BigQuery.

    Returns:
        DataFrame with only the new rows.
    """
    if not existing_keys:
        logging.info("No existing keys provided - keeping all rows.")
        return df
    if df.empty:
        logging.info("Input DataFrame is empty - returning empty DataFrame.")
        return df

    try:
        # Ensure 'ingestionDate' in df is comparable (convert to date objects)
        if pd.api.types.is_datetime64_any_dtype(df['ingestionDate']):
             df_dates = df['ingestionDate'].dt.date
        else:
             # Attempt conversion if not already datetime
             try:
                 df_dates = pd.to_datetime(df['ingestionDate'], errors='coerce').dt.date
             except Exception:
                 logging.error("Could not convert 'ingestionDate' column to date objects for filtering.")
                 raise # Or handle differently

        # Create a set of existing keys for efficient lookup
        # Ensure keys in the set are (str, date)
        existing_keys_set = set((str(k[0]), k[1]) for k in existing_keys if isinstance(k[1], date))


        # Create tuples from the DataFrame rows for comparison
        # Ensure keys are (str, date) and handle potential NaT in dates
        df_key_tuples = list(zip(df['job_id'].astype(str), df_dates))

        # Create a boolean mask: True if the tuple is *not* in the existing set
        # Handle potential NaT dates during comparison
        keep_mask = [
             (job_id, dt) not in existing_keys_set if pd.notna(dt) else True # Keep rows with invalid dates for now? Or False?
             for job_id, dt in df_key_tuples
        ]


        initial_count = len(df)
        filtered_df = df[keep_mask].reset_index(drop=True)
        removed_count = initial_count - len(filtered_df)

        logging.info(f"Filtered out {removed_count} duplicate rows based on (job_id, ingestionDate), keeping {len(filtered_df)} new rows.")
        return filtered_df

    except Exception as e:
        logging.error(f"Error filtering out duplicates: {e}", exc_info=True)
        raise

def load_to_bigquery(df: pd.DataFrame) -> None:
    """
    Loads the DataFrame to the configured LinkedIn BigQuery table.

    Args:
        df: The DataFrame to load.
    """
    bq_project_id = os.getenv("LINKEDIN_BQ_PROJECT_ID") # Let get_bigquery_client handle fallback logic
    bq_dataset_id = os.getenv("LINKEDIN_BQ_DATASET_ID")
    bq_table_id = os.getenv("LINKEDIN_BQ_TABLE_ID")
    # Default write/create dispositions suitable for appending new data
    bq_write_disposition = os.getenv("LINKEDIN_BQ_WRITE_DISPOSITION", "WRITE_APPEND")
    bq_create_disposition = os.getenv("LINKEDIN_BQ_CREATE_DISPOSITION", "CREATE_NEVER") # Safer default

    # Basic validation of necessary IDs
    if not all([bq_dataset_id, bq_table_id]):
        raise ValueError("Missing required BigQuery config (LINKEDIN_BQ_DATASET_ID, LINKEDIN_BQ_TABLE_ID)")

    if df.empty:
        logging.info("DataFrame is empty. No data to load to BigQuery.")
        return

    try:
        # --- Use the authentication helper function ---
        bq_client = get_bigquery_client()
        # ---

        # Construct full table ID using the client's project if needed
        final_project_id = bq_project_id or bq_client.project
        if not final_project_id:
             raise ValueError("Could not determine BigQuery Project ID.")
        full_table_id = f"{final_project_id}.{bq_dataset_id}.{bq_table_id}"

        logging.info(f"Attempting to load {len(df)} rows to BigQuery table: {full_table_id}")


        # Define job configuration
        job_config = bigquery.LoadJobConfig(
            write_disposition=bq_write_disposition,
            create_disposition=bq_create_disposition,
            # Let BQ infer schema from DataFrame, ensure DataFrame matches target
            autodetect=False, # Explicitly False is safer if schema is known
            # Define schema based on your final_columns in process_linkedin_job_data
            # This provides better validation and control.
            schema=[
                bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("job_title", "STRING"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("employment_type", "STRING"),
                bigquery.SchemaField("experience_level", "STRING"),
                bigquery.SchemaField("workplace_type", "STRING"),
                bigquery.SchemaField("applicant_count", "STRING"), # Keep as STRING if inconsistent numbers/text
                bigquery.SchemaField("reposted_info", "STRING"),
                bigquery.SchemaField("skills_summary", "STRING"),
                bigquery.SchemaField("application_type", "STRING"),
                bigquery.SchemaField("job_description", "STRING"),
                bigquery.SchemaField("job_link", "STRING"),
                bigquery.SchemaField("company_logo_url", "STRING"),
                bigquery.SchemaField("source_file", "STRING"),
                bigquery.SchemaField("ingestionDate", "DATE", mode="REQUIRED") # Define as DATE
            ],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingestionDate" # Match the partition column name
            ),
            # Consider adding clustering fields if beneficial for queries
            clustering_fields=["job_id"]
        )

        # Ensure ingestionDate is compatible (pandas Timestamp or datetime.date)
        # Already converted to datetime in process_linkedin_job_data
        # df['ingestionDate'] = df['ingestionDate'].dt.date # Convert to date objects if necessary before load

        # Load DataFrame to BigQuery
        job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        logging.info(f"Load job started: {job.job_id}")
        job.result()  # Wait for job to complete

        # Verify load (optional, but good practice)
        if job.errors:
             logging.error(f"BigQuery load job failed with errors: {job.errors}")
             # Raise an exception to halt execution if errors occurred
             raise Exception(f"BigQuery load job {job.job_id} failed: {job.errors}")
        else:
            table = bq_client.get_table(full_table_id) # Fetch updated table info
            logging.info(f"Successfully loaded {job.output_rows} rows to {full_table_id}. Table now has {table.num_rows} total rows.")

    except Exception as e:
        logging.error(f"Error loading data to BigQuery table {full_table_id}: {e}", exc_info=True)
        # Attempt to log specific BQ errors if available from a failed job object
        if 'job' in locals() and hasattr(job, 'errors') and job.errors:
             for error in job.errors:
                 logging.error(f"BigQuery error detail: {error['message']}")
        raise # Re-raise the exception

# --- Main Execution Logic (Example) ---
if __name__ == "__main__":
    # Example usage: Replace with your actual environment variable setup or config loading
    # Assumes environment variables like GCS_BUCKET_NAME, GCS_LINKEDIN_SUBFOLDER_PATH,
    # LINKEDIN_BQ_PROJECT_ID, LINKEDIN_BQ_DATASET_ID, LINKEDIN_BQ_TABLE_ID,
    # and credential variables (or GOOGLE_APPLICATION_CREDENTIALS) are set.
    load_dotenv()

    gcs_bucket = os.getenv("GCS_BUCKET_NAME")
    gcs_prefix = os.getenv("GCS_SUBFOLDER_PATH")

    if not gcs_bucket or not gcs_prefix:
        logging.error("Required environment variables GCS_BUCKET_NAME or GCS_LINKEDIN_SUBFOLDER_PATH are not set.")
        exit(1) # Or handle appropriately

    try:
        # 1. Find the latest file
        latest_file_path = find_latest_file_in_gcs_by_name(gcs_bucket, gcs_prefix)

        if not latest_file_path:
            logging.warning("No new file found to process.")
            exit(0) # Or handle appropriately

        # 2. Extract ingestion date from filename
        ingestion_dt = extract_date_from_filename(latest_file_path)
        if not ingestion_dt:
            logging.error(f"Could not extract date from filename: {latest_file_path}. Skipping load.")
            exit(1) # Or handle appropriately

        # 3. Read data from GCS
        raw_df = read_parquet_from_gcs(latest_file_path)

        # 4. Process/Transform data
        processed_df = process_linkedin_job_data(raw_df, ingestion_dt)

        if not processed_df.empty:
            # 5. Check for existing keys in BigQuery
            existing_keys_in_bq = check_existing_keys_in_bigquery(processed_df)

            # 6. Filter out duplicates
            df_to_load = filter_out_duplicates(processed_df, existing_keys_in_bq)

            # 7. Load new data to BigQuery
            load_to_bigquery(df_to_load)
        else:
            logging.info("Processed DataFrame is empty after transformation. Nothing to load.")

        logging.info("LinkedIn data processing pipeline finished successfully.")

    except Exception as e:
        logging.error(f"LinkedIn data processing pipeline failed: {e}", exc_info=True)
        exit(1) # Indicate failure
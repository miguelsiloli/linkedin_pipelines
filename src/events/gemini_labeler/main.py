# src/main.py
import os
import time
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from google.cloud import bigquery # Import BigQuery

# Import modules using relative paths within the 'src' package
from . import config
from . import gemini_processor
from . import file_utils # Imports the updated file_utils with BQ helpers

def main():
    """Main function to read new jobs from BigQuery, process descriptions, and upload to BQ."""
    load_dotenv()
    print("Starting differential job description processing and BQ upload...")
    start_time = time.time()

    # 1. Initialize Clients
    gemini_client = gemini_processor.create_gemini_client()
    bq_client = file_utils.get_bigquery_client()

    # 2. Output Directory (Optional: If still needed for logs/backups)
    if config.OUTPUT_DIRECTORY:
        try:
            file_utils.create_output_directory(config.OUTPUT_DIRECTORY)
            print(f"Optional: Using local directory for backup/logging: {config.OUTPUT_DIRECTORY}")
        except Exception as e:
            print(f"Warning: Could not create output directory '{config.OUTPUT_DIRECTORY}'. Continuing without local backup. Error: {e}")
            # Decide if this is critical; maybe set config.OUTPUT_DIRECTORY = None here?

    # 3. Differential Read from BigQuery
    print(f"Identifying new jobs by comparing '{config.SOURCE_TABLE_FULL_ID}' and '{config.AUGMENTED_TABLE_FULL_ID}'...")
    print(f"Using '{config.JOB_ID}' column for source IDs.")
    # *** IMPORTANT: Pass the correct column name for the *augmented* table's ID ***
    print(f"Using '{config.LINKEDIN_BQ_AUGMENTED_JOB_LINK_COLUMN}' column for processed IDs.")

    source_job_ids = file_utils.get_distinct_job_ids_from_bq(
        bq_client,
        config.SOURCE_TABLE_FULL_ID,
        config.JOB_ID # ID column in source table
    )
    if not source_job_ids and not file_utils.get_distinct_job_ids_from_bq: # Check if source is truly empty or errored
         print(f"Warning: No job IDs found in source table '{config.SOURCE_TABLE_FULL_ID}' or error occurred during fetch. Cannot proceed.")
         exit(1)

    processed_job_ids = file_utils.get_distinct_job_ids_from_bq(
        bq_client,
        config.AUGMENTED_TABLE_FULL_ID,
        config.LINKEDIN_BQ_AUGMENTED_JOB_LINK_COLUMN # ID column in augmented table
    )
    # If processed_job_ids is empty (e.g., table doesn't exist yet or error), all source_job_ids will be considered new.

    new_job_ids = source_job_ids - processed_job_ids

    if not new_job_ids:
        print("No new jobs found to process.")
        # ... (rest of the no new jobs summary) ...
        exit(0)

    print(f"Found {len(new_job_ids)} new jobs to process.")

    df = file_utils.get_jobs_to_process_from_bq(
        bq_client,
        config.SOURCE_TABLE_FULL_ID,
        config.JOB_ID,
        config.JOB_DESCRIPTION_COLUMN,
        new_job_ids
    )

    if df is None or df.empty:
        print("Error fetching details for new jobs, or no details found. Aborting.")
        exit(1)

    print(f"Successfully fetched {len(df)} rows for processing.")

    # 4. Process Each NEW Job Description and Upload
    processed_successfully_count = 0
    uploaded_successfully_count = 0
    skipped_count = 0
    processing_error_count = 0
    upload_error_count = 0
    api_calls_made = 0

    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing & Uploading Jobs"):
        job_description = row[config.JOB_DESCRIPTION_COLUMN]

        # --- Add Delay ---
        if api_calls_made > 0:
             time.sleep(config.REQUEST_DELAY_SECONDS)

        # --- Process with Gemini ---
        extracted_data_dict = None # Ensure it's defined before try block
        try:
            # Assume gemini_processor handles its own internal errors and returns None on failure
            extracted_data_dict = gemini_processor.process_job_description(gemini_client, job_description)
            extracted_data_dict["job_id"] = row["job_id"] # need to add job id before validation
            if extracted_data_dict:
                api_calls_made += 1 # Increment only on successful API call/parse
                processed_successfully_count += 1 # Count successful processing
            else:
                 # Error message should be logged by gemini_processor
                 tqdm.write(f"Failed to process description for job link: {row["job_id"]}")
                 processing_error_count += 1
                 continue # Skip upload if processing failed

        except Exception as e:
             # Catch unexpected errors during the processing call itself
             tqdm.write(f"Unexpected error during Gemini processing for job link {row["job_id"]}: {e}")
             processing_error_count += 1
             continue # Skip upload

        # --- Upload to BigQuery ---
        if extracted_data_dict: # Only attempt upload if processing was successful
            extracted_data_dict = file_utils.ensure_schema_compliance(data = extracted_data_dict, template=config.DEFAULT_BQ_SCHEMA_TEMPLATE)
            upload_success = file_utils.upload_processed_data_to_bq(
                client=bq_client,
                table_full_id=config.AUGMENTED_TABLE_FULL_ID,
                processed_data=extracted_data_dict,
                job_link=row["job_id"],
                job_link_column_name=config.LINKEDIN_BQ_AUGMENTED_JOB_LINK_COLUMN
            )

            if upload_success:
                uploaded_successfully_count += 1
            else:
                # Error message logged by upload_processed_data_to_bq
                upload_error_count += 1
                # Optional: Still save locally even if upload failed? Depends on requirements.
                # if output_filepath:
                #     try:
                #         file_utils.save_json(extracted_data_dict, output_filepath)
                #         tqdm.write(f"Saved local backup for {job_link_raw} despite BQ upload failure.")
                #     except Exception as e:
                #         tqdm.write(f"Warning: Failed BQ upload AND failed local save for {job_link_raw}: {e}")


    # 5. Print Summary
    end_time = time.time()
    total_time = end_time - start_time
    print("\n--- Processing & Upload Summary ---")
    # ... (summary of differential read counts) ...
    print(f"New jobs identified for processing: {len(new_job_ids)}")
    print(f"Rows fetched for processing: {len(df)}")
    print(f"--- Run Results ---")
    print(f"Successfully processed by Gemini: {processed_successfully_count}")
    print(f"Successfully uploaded to BigQuery: {uploaded_successfully_count}")
    print(f"Skipped (invalid input/ID): {skipped_count}")
    print(f"Errors during Gemini processing: {processing_error_count}")
    print(f"Errors during BigQuery upload: {upload_error_count}")
    print(f"--- Timings & API ---")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Total Gemini API calls attempted/successful: {api_calls_made}")
    # ... (rest of the summary) ...
    print(f"Augmented data loaded into: '{config.AUGMENTED_TABLE_FULL_ID}'")
    if config.OUTPUT_DIRECTORY:
         print(f"Optional local backups saved to: '{config.OUTPUT_DIRECTORY}'")
    print("---------------------------------")


if __name__ == "__main__":
    # Ensure google-cloud-bigquery and python-dotenv are installed
    # pip install google-cloud-bigquery python-dotenv pandas pyarrow tqdm google-generativeai
    # Ensure Application Default Credentials (ADC) are set up:
    # gcloud auth application-default login
    # *** ACTION REQUIRED: Update your BigQuery 'linkedin_augmented_staging' table schema ***
    # *** Add a column (e.g., `source_job_link` STRING) to store the original job link ***
    main()
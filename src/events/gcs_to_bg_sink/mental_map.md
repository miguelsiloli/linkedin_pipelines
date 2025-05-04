Mental Map: Scheduled Daily Load - Process Latest LinkedIn Job Parquet File

    Trigger: Scheduled Time Reached

        What: Daily schedule hits (e.g., configured time like 3 AM).

        How: Cloud Scheduler job.

        Action: Starts the Cloud Function/Cloud Run job responsible for LinkedIn data ingestion.

    Find the Latest File

        What: Identify the single most recent Parquet file within the specific GCS directory for LinkedIn job data (e.g., gs://your-bucket-name/staging/linkedin_jobs/).

        How:

            List all objects in the specified GCS_LINKEDIN_SUBFOLDER_PATH.

            Filter for files matching the pattern linkedin_jobs_data_*.parquet.

            Sort the list of matching Parquet files by filename. Since the date YYYY-MM-DD is at the end before the extension, standard alphabetical sorting will correctly identify the latest file (e.g., linkedin_jobs_data_2025-04-30.parquet comes after linkedin_jobs_data_2025-04-29.parquet).

            Select the last file from the sorted list.

        Important:

            The filename convention linkedin_jobs_data_YYYY-MM-DD.parquet is crucial for reliable sorting.

            Handle the case where no matching Parquet files are found (log and exit gracefully).

            Store the filename of the selected latest file – you'll need it to parse the date.

    Extract Ingestion Date from Filename

        What: Get the date part from the filename identified in Step 2.

        How:

            Take the filename (e.g., linkedin_jobs_data_2025-04-30.parquet).

            Use string manipulation or regular expressions to extract the date part (2025-04-30).

            Convert this string into a proper Date or Timestamp data type (e.g., BigQuery DATE). Let's call this parsed_ingestion_date.

    Process Target File

        What: Read the specific latest file identified in Step 2 from GCS into memory (e.g., using Pandas or PySpark).

        How: Use the GCS URI of the identified file.

    Prep Data

        What: Cleanse columns, fix data types, and add the ingestionDate.

        How:

            Perform standard data cleaning (handle nulls, trim strings, etc.) on the data read in Step 4.

            Ensure job_id column has the correct data type (likely String).

            Add a new column named ingestionDate to the DataFrame/data structure, populating it with the parsed_ingestion_date obtained in Step 3 for all rows read from this file.

    Identify Keys in Target File

        What: Get all unique (job_id, ingestionDate) pairs from the data just processed (which now includes the ingestionDate column).

        How: Extract the job_id and the newly added ingestionDate columns, drop duplicates based on this composite key.

    Check BigQuery for Existing Keys

        What: Ask BigQuery: "Do any of the (job_id, ingestionDate) pairs I found in step 6 already exist in the linkedin_jobs_staging table (or your designated target table)?"

        How: Run a targeted SQL query against the BigQuery target table using the keys from step 6 (e.g., SELECT job_id, ingestionDate FROM your_project.your_dataset.linkedin_jobs_staging WHERE (job_id, ingestionDate) IN UNNEST(@keys_from_step_6)).

        Important: Get back a list/set of keys that do exist in BigQuery.

    Filter Out Duplicates

        What: Remove rows from the processed data (in memory) whose (job_id, ingestionDate) pair was found in BigQuery (result of step 7).

        How: Filter the DataFrame/data structure.

        Result: Only rows whose specific (job_id, ingestionDate) combination is not already in the target BigQuery table remain.

    Append New Rows to BigQuery

        What: Load the filtered (new-only) rows into the BigQuery linkedin_jobs_staging table.

        How: Use the BigQuery client library's load function.

        Important:

            Use WRITE_APPEND.

            Ensure CREATE_DISPOSITION is set to CREATE_NEVER (assuming the table exists).

            Verify the schema of the data being loaded (including the added ingestionDate column) matches the target BigQuery table schema.
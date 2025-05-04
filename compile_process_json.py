import json
import os
from pathlib import Path
import sys
import pandas as pd # Added pandas import

def process_json_outer_join(directory_path_str: str):
    """
    Reads all JSON files in a directory, performs an outer join on their keys,
    and returns a list of dictionaries with all keys present.

    Args:
        directory_path_str: The path to the directory containing JSON files.

    Returns:
        A list of dictionaries, where each dictionary represents a processed
        JSON file containing all unique keys found across all files.
        Returns an empty list if the directory is not found or no valid
        JSON files are processed.
    """
    directory_path = Path(directory_path_str)
    if not directory_path.is_dir():
        print(f"Error: Directory not found: {directory_path_str}", file=sys.stderr)
        return []

    all_keys = set()
    original_data_list = []
    processed_files_count = 0
    skipped_files = []

    print(f"Processing JSON files in: {directory_path.resolve()}")

    # --- First Pass: Read files, validate, collect keys and original data ---
    for file_path in directory_path.glob('*.json'):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if isinstance(data, dict):
                # Store original data along with source file name
                original_data_list.append({"source_file": file_path.name, "data": data})
                all_keys.update(data.keys())
                processed_files_count += 1
            else:
                print(f"Warning: Skipping '{file_path.name}'. Content is not a JSON object (dictionary).", file=sys.stderr)
                skipped_files.append(file_path.name)

        except json.JSONDecodeError:
            print(f"Warning: Skipping '{file_path.name}'. Invalid JSON format.", file=sys.stderr)
            skipped_files.append(file_path.name)
        except IOError as e:
            print(f"Warning: Skipping '{file_path.name}'. Could not read file: {e}", file=sys.stderr)
            skipped_files.append(file_path.name)
        except Exception as e:
             print(f"Warning: Skipping '{file_path.name}'. An unexpected error occurred: {e}", file=sys.stderr)
             skipped_files.append(file_path.name)


    if not original_data_list:
        print("No valid JSON object files found or processed.", file=sys.stderr)
        return []

    print(f"\nProcessed {processed_files_count} valid JSON files.")
    if skipped_files:
        print(f"Skipped {len(skipped_files)} files: {', '.join(skipped_files)}")

    print(f"\nFound {len(all_keys)} unique keys across all files.")
    # print("All unique keys:", sorted(list(all_keys))) # Optional: Print all keys

    # --- Second Pass: Construct the final list with outer joined keys ---
    output_list = []
    # Add 'source_file' key explicitly if you want it in the final output
    all_keys_with_meta = sorted(list(all_keys))
    output_keys_order = ['source_file'] + all_keys_with_meta # Define column order

    for item in original_data_list:
        processed_record = {"source_file": item["source_file"]} # Start with source file
        original_file_data = item["data"]

        for key in all_keys_with_meta: # Iterate through data keys only
            # Use .get(key, None) which returns None if the key doesn't exist
            processed_record[key] = original_file_data.get(key, None)

        output_list.append(processed_record)

    # Ensure consistent key order for DataFrame creation (optional but good practice)
    # This step might not be strictly necessary if pandas handles it, but it's explicit.
    final_ordered_list = []
    for record in output_list:
        ordered_record = {key: record.get(key) for key in output_keys_order}
        final_ordered_list.append(ordered_record)


    return final_ordered_list # Return the list of dictionaries

# --- Main Execution ---
if __name__ == "__main__":
    TARGET_DIRECTORY = "processed_jobs" # <-- Set your directory name here
    OUTPUT_FILENAME_PARQUET = "processed_jobs_outer_join.parquet" # <-- Output Parquet filename

    print("Starting JSON processing...")
    # Make sure pandas and pyarrow (or fastparquet) are installed:
    # pip install pandas pyarrow
    try:
        import pandas as pd
    except ImportError:
        print("\nError: pandas library not found. Please install it:", file=sys.stderr)
        print("pip install pandas pyarrow", file=sys.stderr)
        sys.exit(1)

    try:
        # Check for a parquet engine - pyarrow is preferred
        pd.DataFrame().to_parquet(os.devnull, engine='pyarrow')
    except ImportError:
         try:
             # Check for fastparquet as a fallback
             pd.DataFrame().to_parquet(os.devnull, engine='fastparquet')
             print("Using 'fastparquet' engine.")
         except ImportError:
            print("\nError: No Parquet engine found (pyarrow or fastparquet). Please install one:", file=sys.stderr)
            print("pip install pyarrow  (recommended)", file=sys.stderr)
            print("or: pip install fastparquet", file=sys.stderr)
            sys.exit(1)
    except Exception: # Catch other potential errors during the dummy write test
        pass # Assume engine is okay if check fails for non-ImportError reasons


    results_list = process_json_outer_join(TARGET_DIRECTORY)

    if results_list:
        print(f"\n--- Processing Complete ({len(results_list)} records) ---")

        # Optional: Display first few results (still useful for quick check)
        print("First 5 records (as dictionaries):")
        output_limit = 5
        # Use json.dumps for pretty printing the dictionaries to console
        print(json.dumps(results_list[:output_limit], indent=2, default=str)) # Use default=str for non-serializable types if any
        if len(results_list) > output_limit:
            print(f"\n(... and {len(results_list) - output_limit} more records)")

        # --- Convert to DataFrame and Save as Parquet ---
        try:
            print(f"\nConverting {len(results_list)} records to DataFrame...")
            # Create DataFrame directly from the list of dictionaries
            df = pd.DataFrame(results_list)

            # Optional: Explicitly set column order based on earlier definition
            # df = df[output_keys_order] # Uncomment if you want strict column order

            print(f"Saving DataFrame to Parquet file: '{OUTPUT_FILENAME_PARQUET}'")
            # Save to Parquet, index=False prevents saving the pandas index as a column
            df.to_parquet(OUTPUT_FILENAME_PARQUET, index=False, engine='pyarrow') # Or engine='fastparquet'
            print(f"\nSuccessfully saved all {len(results_list)} processed records to '{OUTPUT_FILENAME_PARQUET}'")

        except Exception as e: # Catch potential errors during DataFrame creation or Parquet writing
            print(f"\nError during DataFrame conversion or Parquet saving: {e}", file=sys.stderr)
            print("Please ensure pandas and a Parquet engine (pyarrow/fastparquet) are correctly installed.", file=sys.stderr)

    else:
        print("\nNo results generated.")
import pandas as pd
from bs4 import BeautifulSoup
import os
from pathlib import Path
import re
from datetime import datetime, timezone
import logging
import io
from typing import List, Dict, Any, Optional, Generator, Tuple
import json

# --- Google Cloud Imports ---
from google.cloud import storage
from google.oauth2 import service_account

# --- Local Imports ---
import config  # Import the configuration file

# --- Environment Variables ---
from dotenv import load_dotenv

# --- Prefect Imports ---
from prefect import flow, task, get_run_logger
# from functools import lru_cache

# Load environment variables from .env file
load_dotenv()

# --- Configure Logging ---
logging.basicConfig(level=config.LOGGING_LEVEL)
logger = logging.getLogger(__name__)

# --- Precompiled Regular Expressions ---
JOB_ID_REGEX = re.compile(r'(?:/view/|/jobs/|/postings/|/opportunities/)(\d{8,})/?')
FALLBACK_JOB_ID_REGEX = re.compile(r'/(\d{10,})')
LESS_SPECIFIC_JOB_ID_REGEX = re.compile(r'(\d{8,})')

# --- Cache for GCS credentials ---
_gcs_credentials = None

class LinkedInJobParser:
    """Class to handle LinkedIn job HTML parsing with better performance."""
    
    def __init__(self, 
                 input_dir: Path, 
                 output_dir: Path, 
                 output_filename: str, 
                 html_parser: str = 'html.parser'):
        """
        Initialize the parser with configuration.
        
        Args:
            input_dir: Directory containing HTML files to process
            output_dir: Directory where output will be saved
            output_filename: Name of the output file
            html_parser: Parser to use with BeautifulSoup
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_filename = output_filename
        self.html_parser = html_parser
        self.selectors = config.SELECTORS
        self.logger = logger
        self.output_path = output_dir / output_filename
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Preload selectors for better performance
        self._prepare_selectors()
    
    def _prepare_selectors(self) -> None:
        """
        Prepare and optimize selectors for extraction.
        Group selectors by extraction type for efficiency.
        """
        # Group selectors by extraction type to minimize DOM traversals
        self.text_selectors = {}
        self.get_text_selectors = {}
        self.attribute_selectors = {}
        
        for field, selector in self.selectors.items():
            if field == 'company_logo_url':
                self.attribute_selectors[field] = (selector, 'src')
            elif field == 'job_description':
                self.get_text_selectors[field] = selector
            else:
                self.text_selectors[field] = selector
    
    def find_html_files(self) -> List[Path]:
        """
        Find all HTML files in the input directory more efficiently.
        """
        if not self.input_dir.is_dir():
            self.logger.error(f"Input directory not found: {self.input_dir}")
            return []
        
        # Use set to avoid duplicates, more memory-efficient for large directories
        html_files = set()
        
        # Add HTML files from directory (non-recursive for efficiency)
        html_files.update(self.input_dir.glob('*.html'))
        html_files.update(self.input_dir.glob('*.htm'))
        
        files_list = list(html_files)
        
        if not files_list:
            self.logger.warning(f"No HTML files found in {self.input_dir}")
        else:
            self.logger.info(f"Found {len(files_list)} HTML files in {self.input_dir}")
            
        return files_list
    
    @staticmethod
    def extract_job_id(link: Optional[str]) -> Optional[str]:
        """
        Extract job ID from a LinkedIn job link string more efficiently.
        Uses precompiled regex patterns for better performance.
        """
        if not link:
            return None
        
        # Try with the main regex first
        match = JOB_ID_REGEX.search(link)
        if match:
            return match.group(1)
        
        # Try fallback patterns if main pattern fails
        try:
            path_part = link.split('?')[0]
            fallback_match = FALLBACK_JOB_ID_REGEX.search(path_part)
            if fallback_match:
                return fallback_match.group(1)
                
            less_specific_match = LESS_SPECIFIC_JOB_ID_REGEX.search(path_part)
            if less_specific_match:
                return less_specific_match.group(1)
        except Exception:
            pass
        
        logger.warning(f"Could not extract Job ID from link: {link}")
        return None
    
    def process_html_batch(self, html_files: List[Path], batch_size: int = 10) -> Generator[List[Dict], None, None]:
        """
        Process HTML files in batches for better efficiency.
        
        Args:
            html_files: List of HTML file paths to process
            batch_size: Number of files to process in each batch
            
        Yields:
            Batches of processed job data dictionaries
        """
        total_files = len(html_files)
        self.logger.info(f"Processing {total_files} files in batches of {batch_size}")
        
        for i in range(0, total_files, batch_size):
            batch_files = html_files[i:i+batch_size]
            batch_results = []
            
            for file_path in batch_files:
                try:
                    job_data = self.process_single_file(file_path)
                    if job_data:
                        batch_results.append(job_data)
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path.name}: {e}", 
                                      exc_info=config.LOGGING_LEVEL)
            
            if batch_results:
                yield batch_results
                
            # Progress logging for long-running processes
            self.logger.debug(f"Processed batch {i//batch_size + 1}/{(total_files + batch_size - 1)//batch_size}")
    
    def process_single_file(self, file_path: Path) -> Optional[Dict]:
        """
        Process a single HTML file and extract job data more efficiently.
        """
        self.logger.debug(f"Processing file: {file_path.name}")
        
        try:
            # Read the HTML content - use a context manager for proper resource handling
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Quick validation check before parsing
            if not html_content or '<html' not in html_content.lower():
                self.logger.warning(f"File {file_path.name} seems empty or not valid HTML.")
                return None
            
            # Parse HTML once for all extractions
            soup = BeautifulSoup(html_content, self.html_parser)
            
            # Initialize with the source file
            job_data = {'source_file': file_path.name}
            
            # Extract job link and ID first
            link_selector = self.selectors.get('job_link')
            link_element = soup.select_one(link_selector) if link_selector else None
            job_link = link_element.get('href') if link_element else None
            job_data['job_link'] = job_link
            job_data['job_id'] = self.extract_job_id(job_link)
            
            # Extract text fields in one pass
            for field, selector in self.text_selectors.items():
                element = soup.select_one(selector)
                job_data[field] = element.get_text(strip=True) if element else None
            
            # Extract multi-line text fields
            for field, selector in self.get_text_selectors.items():
                element = soup.select_one(selector)
                job_data[field] = element.get_text(separator='\n', strip=True) if element else None
            
            # Extract attribute fields
            for field, (selector, attribute) in self.attribute_selectors.items():
                element = soup.select_one(selector)
                job_data[field] = element.get(attribute) if element else None
            
            # Validation - only keep records with job_id
            if not job_data.get('job_id'):
                self.logger.warning(f"Missing job_id for file {file_path.name}")
                if not config.KEEP_RECORDS_WITHOUT_JOB_ID:
                    return None
            
            return job_data
            
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing file {file_path.name}: {e}", 
                             exc_info=config.LOGGING_LEVEL)
            return None
    
    def create_dataframe_from_batches(self, data_batches: Generator[List[Dict], None, None]) -> Optional[pd.DataFrame]:
        """
        Create a DataFrame from batches of job data more efficiently.
        
        Args:
            data_batches: Generator yielding batches of job data dictionaries
            
        Returns:
            DataFrame containing all job data or None if no data
        """
        # Define the column order to optimize memory usage
        column_order = [
            'job_id', 'job_title', 'company_name', 'location', 'employment_type',
            'experience_level', 'workplace_type', 'applicant_count', 'reposted_info',
            'skills_summary', 'application_type', 'job_description', 'job_link',
            'company_logo_url', 'source_file'
        ]
        
        # Start with an empty list of dataframes
        dataframes = []
        record_count = 0
        
        # Create a dataframe for each batch and collect them
        for batch in data_batches:
            if batch:
                batch_df = pd.DataFrame(batch)
                dataframes.append(batch_df)
                record_count += len(batch_df)
        
        if not dataframes:
            self.logger.warning("No valid job data collected to create DataFrame.")
            return None
        
        # Combine all dataframes at once
        self.logger.info(f"Combining {len(dataframes)} batches with {record_count} total records")
        df = pd.concat(dataframes, ignore_index=True)
        
        # Reorder columns - only use columns that exist
        existing_columns = [col for col in column_order if col in df.columns]
        extra_columns = [col for col in df.columns if col not in existing_columns]
        final_column_order = existing_columns + extra_columns
        
        df = df[final_column_order]
        
        self.logger.info(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns.")
        return df
    
    def save_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Save DataFrame to a Parquet file efficiently.
        
        Args:
            df: DataFrame to save
            
        Returns:
            True if save was successful, False otherwise
        """
        if df is None or df.empty:
            self.logger.warning("DataFrame is None or empty. No data to save.")
            return False
        
        try:
            # Ensure output directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save with optimal Parquet settings
            df.to_parquet(
                self.output_path,
                engine='pyarrow',
                index=False,
                compression='snappy',
                # Use row groups for better performance with large files
                row_group_size=100000
            )
            
            self.logger.info(f"Successfully saved DataFrame ({len(df)} rows) to {self.output_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving DataFrame to {self.output_path}: {e}", 
                             exc_info=config.LOGGING_LEVEL)
            return False


def get_gcs_credentials() -> service_account.Credentials:
    """
    Get Google Cloud Storage credentials efficiently using global cache.
    Includes logging for the raw and processed private key from environment variables.

    Returns:
        GCS service account credentials
    """
    global _gcs_credentials

    # Return cached credentials if available
    if _gcs_credentials is not None:
        logger.info("Returning cached GCS credentials.")
        return _gcs_credentials

    logger.info("Loading GCP service account credentials from environment...")

    # --- Debugging: Log the raw private key from os.getenv ---
    raw_private_key_env = os.getenv("PRIVATE_KEY")

    if raw_private_key_env:
        logger.info(f"Raw PRIVATE_KEY from env (first 30 chars): '{raw_private_key_env[:30]}'")
        logger.info(f"Raw PRIVATE_KEY from env (last 30 chars): '{raw_private_key_env[-30:]}'")
        # Check for literal '\\n' (two characters: backslash and n)
        logger.info(f"Raw PRIVATE_KEY from env contains literal '\\\\n': {'\\n' in raw_private_key_env}")
        # Check for actual newline character '\n'
        logger.info(f"Raw PRIVATE_KEY from env contains actual newline: {'\n' in raw_private_key_env}")
    else:
        logger.warning("PRIVATE_KEY environment variable is not set or is empty.")
        # Depending on your logic, you might want to raise an error here immediately
        # if private_key is essential and not caught by the later validation.

    # Process the private key (handle escaped newlines if any)
    # The default "" for os.getenv and then replacing ensures it doesn't fail if PRIVATE_KEY is None
    private_key_processed = os.getenv("PRIVATE_KEY", "").replace('\\n', '\n')
    # --- End Debugging Logs ---

    # Read required fields from environment
    credentials_dict = {
        "type": os.getenv("TYPE"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": private_key_processed, # Use the processed key
        "client_email": os.getenv("CLIENT_EMAIL"),
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI"),
        "token_uri": os.getenv("TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
        "universe_domain": os.getenv("UNIVERSE_DOMAIN") # Often optional, can be None
    }

    # Validate required fields
    # Note: "private_key" might be empty if PRIVATE_KEY env var was empty/missing
    # and the .replace still results in an empty string.
    required_fields = ["type", "project_id", "private_key_id", "private_key", "client_email"]
    for field in required_fields:
        if not credentials_dict.get(field): # Checks for None or empty string
            logger.error(f"Missing required GCP credential field: {field}")
            raise ValueError(f"Missing required GCP credential field: {field}")

    # Create credentials object
    try:
        _gcs_credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        logger.info(f"Successfully loaded GCP credentials for {credentials_dict.get('client_email', 'N/A')}")
        return _gcs_credentials
    except Exception as e:
        # Log the dictionary content (excluding the private key for security) if creation fails
        debug_dict = {k: v for k, v in credentials_dict.items() if k != "private_key"}
        debug_dict["private_key_present_and_non_empty"] = bool(credentials_dict.get("private_key"))
        logger.error(f"Failed to create GCP credentials: {e}. Credentials dict (key redacted): {debug_dict}", exc_info=True)
        raise


def save_data_to_gcs(df: pd.DataFrame, gcs_bucket_name: str) -> str:
    """
    Save DataFrame directly to Google Cloud Storage efficiently.
    
    Args:
        df: DataFrame to save
        gcs_bucket_name: Name of the GCS bucket
        
    Returns:
        GCS URI of the uploaded file or error message
    """
    if df is None or df.empty:
        logger.warning("No data was successfully collected. Skipping GCS upload.")
        return "No data to upload."
    
    try:
        if not gcs_bucket_name:
            logger.error("GCS_BUCKET_NAME was not provided.")
            raise ValueError("GCS_BUCKET_NAME is required.")
        
        # Get output path prefix from environment
        gcs_output_path_prefix = os.getenv("GCS_SUBFOLDER_PATH", config.GCS_OUTPUT_PATH).strip('/')
        
        # Generate filename with timestamp
        today_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        output_filename = f"linkedin_scrap_{today_date_str}.parquet"
        blob_name = f"{gcs_output_path_prefix}/{output_filename}"
        gcs_output_uri = f"gs://{gcs_bucket_name}/{blob_name}"
        
        logger.info(f"Preparing to upload {len(df)} records to {gcs_output_uri}")
        
        # Get GCS credentials
        gcs_credentials = get_gcs_credentials()
        
        # Create buffer for serialization
        parquet_buffer = io.BytesIO()
        
        # Optimize memory by dropping unnecessary columns
        column_to_drop = 'experiments'
        if column_to_drop in df.columns:
            df = df.drop(column_to_drop, axis=1)
            logger.info(f"Dropping '{column_to_drop}' column before saving to Parquet.")
        
        # Serialize to Parquet with optimal settings
        df.to_parquet(
            parquet_buffer, 
            index=False, 
            engine='pyarrow', 
            compression='snappy',
            row_group_size=100000
        )
        
        parquet_buffer.seek(0)
        
        # Calculate buffer size for logging
        buffer_size = parquet_buffer.getbuffer().nbytes
        logger.info(f"Serialized data size: {buffer_size / 1024 / 1024:.2f} MB.")
        
        # Upload to GCS
        storage_client = storage.Client(credentials=gcs_credentials)
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(blob_name)
        
        # Use a longer timeout for large files
        timeout = max(300, buffer_size // (1024 * 1024) * 5)  # 5 seconds per MB
        
        blob.upload_from_file(
            parquet_buffer, 
            content_type='application/parquet',
            timeout=timeout
        )
        
        logger.info(f"Successfully uploaded Parquet file to {gcs_output_uri}")
        return gcs_output_uri
        
    except ImportError as e:
        logger.error(f"Import error during GCS save: {e}. Ensure pandas, pyarrow installed.", 
                    exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to process data or upload Parquet file to GCS: {e}", 
                    exc_info=True)
        raise


@flow(name="LinkedIn Job Parser Flow", log_prints=True)
def linkedin_parser_flow(
    input_dir: Path = config.DEFAULT_INPUT_DIR,
    output_dir: Path = config.DEFAULT_OUTPUT_DIR,
    output_filename: str = config.DEFAULT_OUTPUT_FILENAME,
    upload_to_gcs: bool = True,
    batch_size: int = 500  # Added batch size parameter
):
    """
    Prefect flow to parse LinkedIn job HTML files efficiently without multithreading.
    
    Args:
        input_dir: Directory containing HTML files to process
        output_dir: Directory where output will be saved
        output_filename: Name of the output file
        upload_to_gcs: Whether to upload to GCS
        batch_size: Number of files to process in each batch
    """
    run_logger = get_run_logger()
    run_logger.info(f"Starting LinkedIn Job Parser Flow...")
    run_logger.info(f"Using Input Directory: {input_dir}")
    run_logger.info(f"Using Output Directory: {output_dir}")
    run_logger.info(f"Using Output Filename: {output_filename}")
    run_logger.info(f"Using HTML Parser: {config.DEFAULT_HTML_PARSER}")
    run_logger.info(f"Batch Size: {batch_size}")

    run_logger.info(get_gcs_credentials())
    
    # Get GCS bucket name
    gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
    
    # Create parser instance
    parser = LinkedInJobParser(
        input_dir=input_dir,
        output_dir=output_dir,
        output_filename=output_filename,
        html_parser=config.DEFAULT_HTML_PARSER
    )
    
    # Find HTML files
    html_files = parser.find_html_files()
    
    if not html_files:
        run_logger.error("No HTML files found. Aborting flow.")
        return
    
    # Process files in batches and create DataFrame
    data_batches = parser.process_html_batch(html_files, batch_size=batch_size)
    df = parser.create_dataframe_from_batches(data_batches)
    
    if df is None or df.empty:
        run_logger.warning("No data was successfully processed. Aborting flow.")
        return
    
    # Save locally
    local_save_success = parser.save_dataframe(df)
    
    if local_save_success:
        run_logger.info(f"Output saved locally to {parser.output_path}")
    else:
        run_logger.error("Failed to save the output DataFrame locally.")
    
    # Upload to GCS if requested
    if upload_to_gcs:
        try:
            gcs_uri = save_data_to_gcs(df, gcs_bucket_name)
            run_logger.info(f"Data uploaded to GCS: {gcs_uri}")
        except Exception as e:
            run_logger.error(f"Failed to upload data to GCS: {e}", exc_info=True)
    
    run_logger.info(f"Flow completed successfully with {len(df)} records processed.")


# Main execution block
if __name__ == "__main__":
    logger.info("Running Flow directly from script...")
    # Run the flow with default config values
    linkedin_parser_flow()
# config.py
import os
from pathlib import Path
from datetime import date
import logging

# --- Selectors for LinkedIn Job Page Elements ---
# These define how to find specific data points in the HTML
SELECTORS = {
    'company_name': '.job-details-jobs-unified-top-card__company-name a',
    'company_logo_url': "a[aria-label*='logo'] img.ivm-view-attr__img--centered",
    'job_title': '.job-details-jobs-unified-top-card__job-title h1 a',
    'location': ".job-details-jobs-unified-top-card__tertiary-description-container span[dir='ltr'] > span.tvm__text:first-child",
    'reposted_info': ".job-details-jobs-unified-top-card__tertiary-description-container span[dir='ltr'] > span.tvm__text:nth-child(3)",
    'applicant_count': ".job-details-jobs-unified-top-card__tertiary-description-container span[dir='ltr'] > span.tvm__text:nth-child(5)",
    'workplace_type': "li.job-details-jobs-unified-top-card__job-insight--highlight span.ui-label:first-of-type span[aria-hidden='true']",
    'employment_type': "li.job-details-jobs-unified-top-card__job-insight--highlight span.ui-label:nth-of-type(2) span[aria-hidden='true']",
    'experience_level': "li.job-details-jobs-unified-top-card__job-insight--highlight span[dir='ltr'].job-details-jobs-unified-top-card__job-insight-view-model-secondary",
    'skills_summary': "li.job-details-jobs-unified-top-card__job-insight a[href='#HYM']",
    'application_type': "button.jobs-apply-button span.artdeco-button__text",
    'job_link': '.job-details-jobs-unified-top-card__job-title h1 a',
    'job_description': '#job-details > div.mt4'
}

# --- Default Paths and Naming ---
# Use Path objects for easier manipulation
DEFAULT_INPUT_DIR = Path("src/data/linkedin_job_pages_detailed")
DEFAULT_OUTPUT_DIR = Path("output/parsed_jobs") # Define a default output directory
DEFAULT_OUTPUT_FILENAME = f"linkedin_jobs_data_{date.today().isoformat()}.parquet"

GCS_OUTPUT_PATH = "linkedin/raw"

# --- Technical Settings ---
DEFAULT_HTML_PARSER = 'lxml' # Preferred parser for speed

# --- Logging Configuration ---
# Set the desired logging level. Prefect's logger respects this.
# Options: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
LOGGING_LEVEL = logging.INFO
KEEP_RECORDS_WITHOUT_JOB_ID = "FALSE"
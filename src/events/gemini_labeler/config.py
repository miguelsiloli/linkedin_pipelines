# src/config.py
import os
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

# --- General Configuration ---
# OUTPUT_DIRECTORY = os.getenv("OUTPUT_DIRECTORY", "output_json") # Keep if still saving JSONs locally
OUTPUT_DIRECTORY = "output_json" # Or hardcode if preferred
JOB_DESCRIPTION_COLUMN = os.getenv("JOB_DESCRIPTION_COLUMN", "job_description")
JOB_ID = os.getenv("JOB_LINK_COLUMN", "job_id") # CRUCIAL: This column MUST exist in both BQ tables for comparison

# --- API Configuration ---
# Keep your Gemini API key config here
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") # Example

# --- Processing Configuration ---
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", 5.1)) # Delay between API calls

# --- Input Parquet Configuration (No longer primary input, but keep if needed elsewhere) ---
# PARQUET_FILE_PATH = os.getenv("PARQUET_FILE_PATH", "data/linkedin_jobs.parquet")

# --- Source BigQuery Configuration ---
LINKEDIN_BQ_PROJECT_ID = os.getenv("LINKEDIN_BQ_PROJECT_ID", "poised-space-456813-t0")
LINKEDIN_BQ_DATASET_ID = os.getenv("LINKEDIN_BQ_DATASET_ID", "linkedin")
LINKEDIN_BQ_TABLE_ID = os.getenv("LINKEDIN_BQ_TABLE_ID", "linkedin_jobs_staging") # Source table

GEMINI_API_KEY_ENV_VAR = os.getenv("GEMINI_API_KEY")

# --- Target/Augmented BigQuery Configuration ---
LINKEDIN_BQ_AUGMENTED_TABLE_ID = os.getenv("LINKEDIN_BQ_AUGMENTED_TABLE_ID", "linkedin_augmented_staging") # Processed table
LINKEDIN_BQ_AUGMENTED_JOB_LINK_COLUMN = "job_id"

# --- Construct Full Table IDs ---
SOURCE_TABLE_FULL_ID = f"{LINKEDIN_BQ_PROJECT_ID}.{LINKEDIN_BQ_DATASET_ID}.{LINKEDIN_BQ_TABLE_ID}"
AUGMENTED_TABLE_FULL_ID = f"{LINKEDIN_BQ_PROJECT_ID}.{LINKEDIN_BQ_DATASET_ID}.{LINKEDIN_BQ_AUGMENTED_TABLE_ID}"

# --- Add any other configuration variables as needed ---
# Example: GCP Project for billing/quota if different from BQ project
# GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-for-api-calls")

DEFAULT_BQ_SCHEMA_TEMPLATE = {
    "job_summary": {
        "role_title": None,
        "role_objective": None,
        "role_seniority": None,
        "visa_sponsorship": None
    },
    "company_information": {
        "company_type": None,
        "company_values_keywords": []
    },
    "location_and_work_model": {
        "specification_level": None,
        "remote_status": None,
        "flexibility": [],
        "locations": []
    },
    "required_qualifications": {
        "experience_years_min": None,
        "experience_years_max": None,
        "experience_description": None,
        "education_requirements": None,
        "technical_skills": {
            "programming_languages": {
                "general_purpose": [],
                "scripting_frontend": [],
                "query": [],
                "data_ml_libs": [],
                "platform_runtime": [],
                "configuration": [],
                "other_specialized": []
            },
            "cloud_services_tools": [],
            "cloud_platforms": [],
            "database_technologies": [],
            "data_architecture_concepts": {
                "data_modeling": [],
                "data_storage_paradigms": [],
                "etl_elt_pipelines": [],
                "data_governance_quality": [],
                "architecture_patterns": [],
                "big_data_concepts": [],
                "cloud_data_architecture": [],
                "ml_ai_data_concepts": [],
                "core_principles_optimization": []
            },
            "etl_integration_tools": [],
            "data_visualization_bi_tools": [],
            "devops_mlops_ci_cd_tools": [],
            "orchestration_workflow_tools": [],
            "other_tools": []
        },
        "methodologies_practices": [],
        "soft_skills_keywords": []
    },
    "preferred_qualifications": {
        "skills_keywords": [],
        "other_notes": None
    },
    "role_context": {
        "collaboration_with": [],
        "team_structure": None,
        "project_scope": None,
        "key_responsibilities": []
    },
    "benefits": {
        "training_development": None,
        "learning_platforms": [],
        "paid_time_off_days": None,
        "other_benefits_keywords": []
    },
    # job_id will be added separately in the upload function
    "job_id": None # Don't need it here if added later
}

# IMPORTANT: Double-check this template meticulously against your CREATE TABLE DDL.
# Every single field, especially within nested STRUCTs, must be present.
# The error specifically mentioned 'cloud_services_tools' missing inside 'technical_skills'.
# Make sure 'technical_skills' itself exists within 'required_qualifications' and
# 'cloud_services_tools' exists within 'technical_skills' in this template.
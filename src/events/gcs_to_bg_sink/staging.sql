-- DDL for LinkedIn Job Staging Table
-- Replace `your_project.your_dataset` with your actual project and dataset ID

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.linkedin_jobs_staging`
(
  job_id              STRING    NOT NULL OPTIONS(description="Unique identifier for the job listing from the source."),
  job_title           STRING    OPTIONS(description="Title of the job position."),
  company_name        STRING    OPTIONS(description="Name of the company posting the job."),
  location            STRING    OPTIONS(description="Location of the job (e.g., city, region, country)."),
  employment_type     STRING    OPTIONS(description="Type of employment (e.g., Full-time, Contract)."),
  experience_level    STRING    OPTIONS(description="Required experience level (e.g., Mid-Senior level, Entry level)."),
  workplace_type      STRING    OPTIONS(description="Workplace model (e.g., Hybrid, Remote, On-site)."),
  applicant_count     STRING    OPTIONS(description="Information about applicant engagement (often includes text)."),
  reposted_info       STRING    OPTIONS(description="Information if the job was reposted (e.g., time ago)."),
  skills_summary      STRING    OPTIONS(description="Summary of skills match (source-specific info)."),
  application_type    STRING    OPTIONS(description="How to apply (e.g., Apply, Easy Apply)."),
  job_description     STRING    OPTIONS(description="Full text description of the job."),
  job_link            STRING    OPTIONS(description="URL link to the original job posting."),
  company_logo_url    STRING    OPTIONS(description="URL of the company's logo image."),
  source_file         STRING    OPTIONS(description="Name of the source file from which this record was ingested."),
  ingestionDate       DATE      NOT NULL OPTIONS(description="Date when the data was ingested into the staging table, derived from the source filename.")
)
PARTITION BY
  ingestionDate
CLUSTER BY
  job_id
OPTIONS (
  description = "Staging table for LinkedIn job postings data, ingested daily.",
  labels = [("data_source", "linkedin"), ("pipeline", "daily_load")]
);

-- Optional: Add Primary Key constraint if supported and desired (informational only in BQ)
-- ALTER TABLE `your_project.your_dataset.linkedin_jobs_staging`
-- ADD PRIMARY KEY (job_id, ingestionDate) NOT ENFORCED;
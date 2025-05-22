# LinkedIn Data Pipeline

## Description

This data pipeline automates the process of scraping job postings from LinkedIn, processing the data, and storing it in Google Cloud Storage (GCS) and BigQuery. It uses a combination of Python scripts, Prefect flows, and GitHub Actions to achieve this.

## Events

The pipeline is triggered by the following events:

*   **GCS to BigQuery Sink:** This event is triggered when new data lands in a specific GCS bucket. The `gcs_to_bq_sink.yml` workflow listens for these new files and then triggers the `gcs_to_bq.py` script to load the data into BigQuery for analysis. This allows for near real-time analysis of newly scraped data. The staging.sql file is used to create a staging table in BigQuery, and augmented_staging.sql is used to create a staging table with augmented data from the gemini labeler.
*   **Scrape and Parse to GCS:** This event is triggered on a schedule (defined in the `.github/workflows/scrape_and_parse_gcs.yml` workflow) to scrape LinkedIn job postings using the scripts in `src/events/linkedin_scraper/`. The raw data is then parsed and stored in GCS. The `flow.py` file defines the Prefect flow for this process, orchestrating the scraping, parsing, and storage steps. The `parse_to_gcs` event is responsible for parsing the scraped data and storing it in GCS.
*   **Gemini Labeler:** This event uses the Gemini model to label the job postings. The `gemini_processor.py` file contains the logic for interacting with the Gemini model. The `main.py` file orchestrates the labeling process. The `prompt.py` file contains the prompt used to label the job postings. The `config.py` file contains the configuration for the Gemini Labeler. The `augmented_staging.sql` file is used to create a staging table with augmented data from the gemini labeler.

## GitHub Actions

The following GitHub Actions are used to automate the pipeline:

*   `.github/workflows/gcs_to_bq_sink.yml`: This action automates the process of moving data from GCS to BigQuery. It is triggered when new data lands in a specific GCS bucket.
*   `.github/workflows/scrape_and_parse_gcs.yml`: This action automates the scraping and parsing of LinkedIn job postings and storing them in GCS. It is triggered on a schedule.

## Usage

To use this pipeline, you will need to:

1.  Set up a Google Cloud project with access to GCS and BigQuery.
2.  Configure the necessary environment variables.
3.  Enable the GitHub Actions in your repository.

## Project Structure

```
.
├── .github/workflows/             # GitHub Actions workflows
├── src/
│   ├── events/                    # Event-triggered scripts
│   │   ├── gcs_to_bg_sink/        # GCS to BigQuery sink
│   │   ├── gemini_labeler/        # Gemini Labeler for data augmentation
│   │   └── linkedin_scraper/      # LinkedIn scraper
│   ├── dashboard/                 # Dashboard application
│   └── ...
├── readme.md                     # This file
└── ...
```

## License

[Specify License, e.g., MIT License]
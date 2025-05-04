# main.py
"""
Entry point for the LinkedIn Scraper Prefect Flow.
Loads environment variables, retrieves parameters, and runs the flow.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from flow import linkedin_scrape_flow

# --- Main Execution Block ---
if __name__ == "__main__":

    print("Starting LinkedIn Scraper execution...")

    # --- Get Parameters from Environment Variables ---
    # Provide defaults or raise errors if variables are missing

    # Login Credentials (handle None carefully)
    LINKEDIN_EMAIL = os.getenv("LINKEDIN_EMAIL")
    LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")
    LI_AT_COOKIE = os.getenv("LI_AT_COOKIE") # This is prioritized

    # Search Parameters (Required)
    SEARCH_KEYWORDS = os.getenv("SEARCH_KEYWORDS")
    LOCATION = os.getenv("LOCATION")
    if not SEARCH_KEYWORDS or not LOCATION:
        print("ERROR: Missing required environment variables: SEARCH_KEYWORDS and LOCATION")
        sys.exit(1)

    # Output Directory (Required)
    OUTPUT_DIR = os.getenv("OUTPUT_DIR")
    if not OUTPUT_DIR:
        print("ERROR: Missing required environment variable: OUTPUT_DIR")
        sys.exit(1)

    # Scraping Control Parameters (with defaults)
    try:
        MAX_PAGES_TO_SCRAPE = int(os.getenv("MAX_PAGES_TO_SCRAPE", "5"))
        PAGE_LOAD_TIMEOUT = int(os.getenv("PAGE_LOAD_TIMEOUT", "20")) # Increased default
        INTERACTION_DELAY = float(os.getenv("INTERACTION_DELAY", "4")) # Increased default
        SCROLL_PAUSES_WITHIN_PAGE = int(os.getenv("SCROLL_PAUSES_WITHIN_PAGE", "4")) # Scrolls *within* a page
        DELAY_BETWEEN_SCROLLS = float(os.getenv("DELAY_BETWEEN_SCROLLS", "1.5")) # Delay *between* scroll actions
    except ValueError as e:
        print(f"ERROR: Invalid numeric value in environment variables (check MAX_PAGES, TIMEOUT, DELAYs): {e}")
        sys.exit(1)

    # Basic validation for login method
    if not LI_AT_COOKIE and not (LINKEDIN_EMAIL and LINKEDIN_PASSWORD):
         print("WARNING: No li_at cookie found and email/password pair is incomplete.")
         print("         Attempting run without guaranteed login. Expect failures.")
         # Allow proceeding, but login task will likely fail and log errors.

    print("\n--- Configuration ---")
    print(f"Keywords:         {SEARCH_KEYWORDS}")
    print(f"Location:         {LOCATION}")
    print(f"Output Dir:       {OUTPUT_DIR}")
    print(f"Max Pages:        {MAX_PAGES_TO_SCRAPE}")
    print(f"Timeout (s):      {PAGE_LOAD_TIMEOUT}")
    print(f"Interaction Delay:{INTERACTION_DELAY}")
    print(f"Scroll Pauses:    {SCROLL_PAUSES_WITHIN_PAGE}")
    print(f"Scroll Delay:     {DELAY_BETWEEN_SCROLLS}")
    if LI_AT_COOKIE:
        print("Login Method:     li_at Cookie (Value hidden)")
    elif LINKEDIN_EMAIL:
        print(f"Login Method:     Email ({LINKEDIN_EMAIL}) / Password (Hidden)")
    else:
        print("Login Method:     None configured")
    print("--------------------\n")


    # --- Run the Prefect Flow ---
    # Pass parameters explicitly to the flow function
    linkedin_scrape_flow(
        linkedin_email=LINKEDIN_EMAIL,
        linkedin_password=LINKEDIN_PASSWORD,
        li_at_cookie=LI_AT_COOKIE,
        search_keywords=SEARCH_KEYWORDS,
        location=LOCATION,
        output_dir=OUTPUT_DIR,
        max_pages_to_scrape=MAX_PAGES_TO_SCRAPE,
        page_load_timeout=PAGE_LOAD_TIMEOUT,
        interaction_delay=INTERACTION_DELAY,
        scroll_pauses_within_page=SCROLL_PAUSES_WITHIN_PAGE,
        delay_between_scrolls=DELAY_BETWEEN_SCROLLS,
    )

    print("\nLinkedIn Scraper execution finished.")
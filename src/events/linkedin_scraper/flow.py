# flow.py
"""Prefect flow definition for scraping LinkedIn jobs."""

import time
from pathlib import Path
from prefect import flow, get_run_logger
from selenium.webdriver.remote.webdriver import WebDriver # Specific type hint
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


# Import tasks and helpers from other modules
import config
from webdriver_utils import setup_driver, close_driver_task
from linkedin_actions import (
    login_task,
    search_jobs_task,
    save_job_html_task,
    navigate_next_page_task,
    scroll_down_job_list,
    get_current_page_number
)


@flow(name="LinkedIn Job Scraper Flow")
def linkedin_scrape_flow(
    # Parameters will be passed from main.py
    linkedin_email: str | None,
    linkedin_password: str | None,
    li_at_cookie: str | None,
    search_keywords: str,
    location: str,
    output_dir: str,
    max_pages_to_scrape: int,
    page_load_timeout: int,
    interaction_delay: float,
    scroll_pauses_within_page: int,
    delay_between_scrolls: float,
):
    """
    Orchestrates the LinkedIn job scraping process using defined tasks.
    """
    logger = get_run_logger()
    driver: WebDriver | None = None # Use the specific type hint
    total_jobs_saved = 0

    # Log key parameters being used (avoid logging password directly)
    logger.info(f"Starting scrape flow for keywords: '{search_keywords}' in location: '{location}'")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Max pages: {max_pages_to_scrape}, Timeout: {page_load_timeout}, Interaction Delay: {interaction_delay}")
    if li_at_cookie:
        logger.info("Using li_at cookie for login.")
    elif linkedin_email:
        logger.info(f"Using email ({linkedin_email}) for login.")
    else:
        logger.warning("No valid login method configured (Cookie or Email/Password).")


    try:
        # Submit and wait for driver setup
        driver_future = setup_driver.submit()
        driver = driver_future.result() # Wait for driver setup to complete

        if not driver: # Check if setup failed
             raise Exception("WebDriver setup failed, cannot continue.")

        # Submit and wait for login
        login_successful = login_task.submit(
            driver, linkedin_email, linkedin_password, li_at_cookie,
            page_load_timeout, interaction_delay
        ).result() # Block until login completes

        if not login_successful:
            logger.error("Login failed. Aborting flow.")
            # No need to close driver here, finally block handles it
            return # Stop the flow

        # Submit and wait for search
        search_successful = search_jobs_task.submit(
            driver, search_keywords, location, page_load_timeout, interaction_delay
        ).result() # Block until search completes

        if not search_successful:
            logger.error("Initial job search failed. Aborting flow.")
            # No need to close driver here, finally block handles it
            return # Stop the flow

        # --- Loop Through Pages ---
        current_page_num_for_loop = 1 # Tracks loop iteration
        processed_job_ids_global = set() # Track all processed jobs across pages

        while current_page_num_for_loop <= max_pages_to_scrape:
            actual_page_num = get_current_page_number(driver)
            # Use actual page number if found, otherwise use loop counter for logging
            display_page_num = actual_page_num if actual_page_num is not None else current_page_num_for_loop
            logger.info(f"--- Processing Page {display_page_num} (Attempt {current_page_num_for_loop}) ---")

            processed_job_ids_on_page = set() # Track jobs found/processed *on this specific page load*
            new_jobs_found_in_last_scroll = True
            scroll_attempt = 0
            max_scrolls_this_page = scroll_pauses_within_page + 1 # +1 because we check after scrolling

            # Scroll and process loop within a single page
            while scroll_attempt < max_scrolls_this_page:
                if scroll_attempt > 0 and not new_jobs_found_in_last_scroll:
                    logger.debug(f"No new jobs found in previous scroll pass on page {display_page_num}. Stopping scrolls for this page.")
                    break

                # Find job cards currently visible/loaded
                job_cards = []
                try:
                    # Wait short time for list presence
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located(config.JOB_LIST_SELECTOR))
                    job_cards = driver.find_elements(*config.JOB_CARD_SELECTOR)
                    logger.debug(f"Found {len(job_cards)} potential job cards on screen (Scroll attempt {scroll_attempt+1}/{max_scrolls_this_page}).")
                except (NoSuchElementException, TimeoutException):
                    logger.warning(f"Job card list selector not found or timed out after scroll attempt {scroll_attempt+1}.")
                    # Decide if we should break or continue - let's try scrolling once more maybe? Or break. Let's break.
                    break
                except StaleElementReferenceException:
                    logger.warning("Job card list became stale during search. Retrying find.")
                    time.sleep(1) # Brief pause before retry
                    try:
                        job_cards = driver.find_elements(*config.JOB_CARD_SELECTOR)
                    except Exception as e_retry:
                         logger.error(f"Retry finding job cards failed: {e_retry}")
                         break # Give up if retry fails

                # Identify *new* jobs in the current view
                jobs_to_process_this_pass = []
                new_job_ids_this_pass = set() # Track IDs found *right now*
                for card in job_cards:
                    job_id = None
                    try:
                        job_id = card.get_attribute('data-job-id')
                        if job_id and job_id not in processed_job_ids_global:
                            # Only add if not processed globally AND not already queued in this specific pass
                            if job_id not in new_job_ids_this_pass:
                                jobs_to_process_this_pass.append((card, job_id)) # Keep card reference briefly
                                new_job_ids_this_pass.add(job_id)
                                # Don't add to processed_job_ids_global until *after* processing attempt
                    except StaleElementReferenceException:
                        logger.warning(f"Stale element ref getting job card ID {job_id or '(unknown)'}. Skipping this card.")
                        continue
                    except Exception as e_card:
                         logger.error(f"Unexpected error processing a job card: {e_card}")

                if jobs_to_process_this_pass:
                     logger.info(f"Found {len(jobs_to_process_this_pass)} new job(s) to process in this pass.")
                     new_jobs_found_in_last_scroll = True # We found new things
                else:
                    logger.debug(f"No new unprocessed jobs found in this view (Scroll attempt {scroll_attempt+1}).")
                    new_jobs_found_in_last_scroll = False

                # Click each newly identified job and save HTML
                save_tasks_submitted = [] # Collect save tasks submitted in this pass
                for i, (job_card_element, job_id) in enumerate(jobs_to_process_this_pass):
                    logger.info(f"  Processing job {i+1}/{len(jobs_to_process_this_pass)} (ID: {job_id}) on page {display_page_num}")
                    try:
                        # Use the job_card_element found earlier, but wrap in wait just in case
                        job_element = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable(job_card_element)
                           # Or re-find by ID: EC.element_to_be_clickable((By.CSS_SELECTOR, f"div[data-job-id='{job_id}']"))
                        )

                        # Scroll the specific job element into center view for clicking
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", job_element)
                        time.sleep(0.6 + interaction_delay * 0.1) # Small delay after scroll

                        # Click the job card
                        try:
                            job_element.click()
                        except ElementClickInterceptedException:
                            logger.warning(f"  Direct click intercepted for job ID: {job_id}. Trying JS click.")
                            time.sleep(0.3) # Pause before JS click
                            driver.execute_script("arguments[0].click();", job_element)

                        # Wait for the details pane to load/update (wait for title visibility)
                        time.sleep(0.3) # Short pause before wait
                        WebDriverWait(driver, page_load_timeout).until(
                            EC.visibility_of_element_located(config.JOB_DETAIL_TITLE_SELECTOR)
                        )
                        # Optional: Check if the title matches expectation (more robust)
                        # detail_title = driver.find_element(*config.JOB_DETAIL_TITLE_SELECTOR).text

                        time.sleep(interaction_delay * 0.75) # Wait for potential dynamic content in details

                        # Get page source *after* waiting for details
                        current_page_source = driver.page_source

                        # Submit save task asynchronously
                        task_run = save_job_html_task.submit(
                            current_page_source, job_id, display_page_num,
                            search_keywords, location, output_dir
                         )
                        save_tasks_submitted.append(task_run) # Optional: track tasks
                        total_jobs_saved += 1
                        processed_job_ids_on_page.add(job_id) # Mark processed on this page load
                        processed_job_ids_global.add(job_id) # Mark processed globally

                    except TimeoutException:
                        logger.error(f"  Error: Timeout waiting for job element (ID: {job_id}) to be clickable or details pane/title to load.")
                        processed_job_ids_global.add(job_id) # Mark as processed even on failure to avoid retries
                    except ElementClickInterceptedException:
                        logger.error(f"  Error: Click still intercepted for job ID: {job_id} after JS attempt. Skipping.")
                        processed_job_ids_global.add(job_id) # Mark as processed even on failure
                    except StaleElementReferenceException:
                         logger.error(f"  Error: Job element (ID: {job_id}) became stale during processing. Skipping.")
                         processed_job_ids_global.add(job_id) # Mark as processed even on failure
                    except Exception as e:
                        logger.error(f"  Error processing job ID {job_id}: {e}", exc_info=False) # Set exc_info=True for traceback
                        processed_job_ids_global.add(job_id) # Mark as processed even on failure

                # --- Scroll down for next batch within the page ---
                scroll_attempt += 1
                if scroll_attempt < max_scrolls_this_page and new_jobs_found_in_last_scroll:
                    logger.debug(f"Scrolling down page/list (Attempt {scroll_attempt}/{max_scrolls_this_page})")
                    scroll_down_job_list(driver, config.JOB_LIST_SCROLL_CONTAINER, pauses=1, delay=delay_between_scrolls)
                elif not new_jobs_found_in_last_scroll and scroll_attempt < max_scrolls_this_page:
                     logger.debug(f"No new jobs found, attempting one final scroll check ({scroll_attempt}/{max_scrolls_this_page}).")
                     scroll_down_job_list(driver, config.JOB_LIST_SCROLL_CONTAINER, pauses=1, delay=delay_between_scrolls)
                else:
                    logger.debug("Max scroll attempts reached for this page or no new jobs found.")
                    # break # Exit scroll loop handled by while condition

            logger.info(f"Finished processing page {display_page_num}. Found/Processed {len(processed_job_ids_on_page)} unique jobs on this page load.")

            # --- Go to Next Page ---
            if current_page_num_for_loop >= max_pages_to_scrape:
                logger.info(f"Reached maximum page limit ({max_pages_to_scrape}).")
                break

            # Submit and wait for navigation task
            navigation_successful = navigate_next_page_task.submit(
                driver, display_page_num, page_load_timeout, interaction_delay
            ).result() # Block until navigation attempt completes

            if not navigation_successful:
                logger.info("Could not navigate to the next page. Ending scraping.")
                break

            current_page_num_for_loop += 1
            # Wait a bit after successful navigation before starting next page processing
            time.sleep(interaction_delay * 0.5)

        # --- End of Page Loop ---
        processed_pages = current_page_num_for_loop -1
        logger.info(f"\n--- Scraping Flow Finished ---")
        logger.info(f"Attempted to process {processed_pages} pages (or reached end/limit).")
        logger.info(f"Total unique jobs processed across all pages: {len(processed_job_ids_global)}")
        logger.info(f"Attempted to save HTML for {total_jobs_saved} jobs (check logs for individual save errors).")
        logger.info(f"Saved files located in: {Path(output_dir).resolve()}")

    except Exception as e:
        # Log critical errors in the main flow orchestration
        logger.error(f"\nAn critical error occurred in the main flow execution: {e}", exc_info=True)

    finally:
        # Ensure driver is always closed if it was initialized
        if driver:
            close_driver_task.submit(driver) # Submit close task
            logger.info("Submitted WebDriver close task.")
        else:
            logger.info("WebDriver was not initialized or setup failed.")
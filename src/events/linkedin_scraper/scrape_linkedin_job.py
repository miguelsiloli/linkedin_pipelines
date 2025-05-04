import time
import os
import re # For cleaning filenames
import sys # To check if running as main script

# --- Environment Variable Loading ---
# We load dotenv here conditionally if the script is run directly
# Tasks and flows themselves should rely on the environment being set up beforehand
if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        # Load from .env file in the current directory or parent directories
        load_dotenv()
        print("Loaded environment variables from .env file.")
    except ImportError:
        print("dotenv library not found. Skipping .env file loading.")
        print("Ensure required environment variables are set manually.")
    except Exception as e:
        print(f"Error loading .env file: {e}")


from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager
from pathlib import Path
from prefect.cache_policies import NO_CACHE  

# --- Prefect ---
from prefect import flow, task, get_run_logger
# from prefect.tasks import task_input_hash # Uncomment if using caching
# from datetime import timedelta            # Uncomment if using caching

# --- Constants (Selectors - Keep these here) ---
LOGIN_URL = "https://www.linkedin.com/login"
USERNAME_FIELD_ID = "username"
PASSWORD_FIELD_ID = "password"
VERIFY_LOGIN_SELECTOR = (By.ID, "global-nav")

JOBS_URL = "https://www.linkedin.com/jobs/"
KEYWORD_SEARCH_SELECTOR = (By.CSS_SELECTOR, "input[id^='jobs-search-box-keyword-id']")
LOCATION_SEARCH_SELECTOR = (By.CSS_SELECTOR, "input[id^='jobs-search-box-location-id']")

JOB_LIST_SELECTOR = (By.CSS_SELECTOR, ".jobs-search-results-list")
JOB_CARD_SELECTOR = (By.CSS_SELECTOR, "div.job-card-container[data-job-id]")
JOB_DETAIL_PANE_SELECTOR = (By.CSS_SELECTOR, "div.jobs-search__job-details--container")
JOB_DETAIL_TITLE_SELECTOR = (By.CSS_SELECTOR, ".jobs-details-top-card__job-title")

PAGINATION_NEXT_BUTTON_SELECTOR = (By.CSS_SELECTOR, "button[aria-label='View next page']")
CURRENT_PAGE_SELECTOR = (By.CSS_SELECTOR, "li[data-test-pagination-page-btn].active > span")


# --- Helper Function (Not a task, used internally) ---
def _clean_filename(text):
    """Removes characters unsafe for filenames."""
    return re.sub(r'[\\/*?:"<>|]', "", text)

# --- Prefect Tasks (Remain largely the same) ---

@task(name="Setup WebDriver", cache_policy=NO_CACHE)
def setup_driver() -> webdriver.Chrome:
    """Initializes and returns a Chrome WebDriver instance."""
    logger = get_run_logger()
    logger.info("Setting up WebDriver...")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Enable for headless runs
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("--lang=en-US")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    try:
        # Suppress webdriver-manager noise by redirecting its log output
        service = ChromeService(executable_path=ChromeDriverManager().install(), log_output=os.devnull)
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(3)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("WebDriver setup complete.")
        return driver
    except Exception as e:
        logger.error(f"WebDriver setup failed: {e}")
        raise

@task(name="Login to LinkedIn", retries=1, retry_delay_seconds=5, cache_policy=NO_CACHE)
def login_task(driver: webdriver.Chrome, email: str | None, password: str | None, cookie: str | None, timeout: int, interaction_delay: float) -> bool:
    """Logs into LinkedIn using email/password or a cookie passed as arguments."""
    logger = get_run_logger()

    # Prioritize cookie if provided
    if cookie:
        logger.info("Attempting login with li_at cookie...")
        driver.get("https://www.linkedin.com")
        time.sleep(1)
        if not driver.current_url.startswith("https://www.linkedin.com"):
             logger.warning(f"Current URL {driver.current_url} not on linkedin.com, potential issue setting cookie.")
             driver.get("https://www.linkedin.com")
             time.sleep(2)

        try:
            driver.delete_all_cookies() # Start fresh
            driver.add_cookie({
                "name": "li_at", "value": cookie,
                "domain": ".linkedin.com", "path": "/",
                # Add secure/httpOnly if needed, but start without
            })
            logger.info("li_at cookie added.")
            driver.get("https://www.linkedin.com/feed/")
            time.sleep(interaction_delay)
        except Exception as e:
             logger.error(f"Failed to add cookie or navigate after adding: {e}")
             return False

    # Fallback to email/password if cookie is not provided or empty
    elif email and password:
        logger.info("Attempting login with email/password...")
        driver.get(LOGIN_URL)
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.ID, USERNAME_FIELD_ID))
            )
            logger.debug("Login page loaded.")
            email_elem = driver.find_element(By.ID, USERNAME_FIELD_ID)
            email_elem.send_keys(email)
            time.sleep(0.5 + interaction_delay * 0.1) # Small dynamic delay
            password_elem = driver.find_element(By.ID, PASSWORD_FIELD_ID)
            password_elem.send_keys(password)
            time.sleep(0.5 + interaction_delay * 0.1)
            password_elem.send_keys(Keys.RETURN)
            logger.info("Credentials submitted.")
        except TimeoutException:
            logger.error(f"Login page or elements not found within {timeout} seconds.")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during login input: {e}")
            return False
    else:
        logger.error("Login failed: No li_at cookie provided and email/password combination is missing or incomplete.")
        return False # Neither method is available

    # --- Verification ---
    try:
        logger.info("Verifying login success...")
        WebDriverWait(driver, timeout * 2).until( # Longer timeout for verification
            EC.presence_of_element_located(VERIFY_LOGIN_SELECTOR)
        )
        logger.info("Login successful!")
        return True
    except TimeoutException:
        logger.error(f"Login verification failed. Element {VERIFY_LOGIN_SELECTOR} not found.")
        current_url = driver.current_url
        logger.error(f"Current URL: {current_url}")
        if "checkpoint" in current_url or "challenge" in current_url:
            logger.error("LinkedIn is asking for verification (CAPTCHA, phone, etc.). Manual intervention required or script needs adjustment.")
        elif "session_password" in current_url:
             logger.error("Incorrect password suspected.")
        elif LOGIN_URL in current_url:
            logger.error("Still on login page, credentials likely incorrect.")

        # Attempt to save page source for debugging
        debug_filename = "login_failure_page.html"
        try:
            with open(debug_filename, 'w', encoding='utf-8') as f: f.write(driver.page_source)
            logger.info(f"Saved login failure page source to {debug_filename}")
        except Exception as save_err: logger.error(f"Could not save login failure page source: {save_err}")
        return False

@task(name="Search Jobs", cache_policy=NO_CACHE)
def search_jobs_task(driver: webdriver.Chrome, keywords: str, location: str, timeout: int, interaction_delay: float) -> bool:
    """Navigates to jobs page and performs search."""
    logger = get_run_logger()
    logger.info(f"Navigating to LinkedIn Jobs: {JOBS_URL}")
    driver.get(JOBS_URL)
    time.sleep(interaction_delay * 0.5) # Wait for page elements to start rendering

    try:
        logger.info(f"Searching for Keywords: '{keywords}'")
        kw_input = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(KEYWORD_SEARCH_SELECTOR))
        driver.execute_script("arguments[0].value = '';", kw_input) # JS clear
        kw_input.send_keys(keywords)
        time.sleep(0.7)

        logger.info(f"Searching for Location: '{location}'")
        loc_input = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(LOCATION_SEARCH_SELECTOR))
        # Clear location more carefully - click, select all, delete, then send keys
        loc_input.click()
        time.sleep(0.3)
        loc_input.send_keys(Keys.CONTROL + "a") # Select all
        time.sleep(0.2)
        loc_input.send_keys(Keys.DELETE)
        time.sleep(0.3)
        # driver.execute_script("arguments[0].value = '';", loc_input) # JS clear as alternative
        loc_input.send_keys(location)
        time.sleep(1.2)
        loc_input.send_keys(Keys.RETURN)
        logger.info("Search submitted.")
        time.sleep(interaction_delay)
        return True

    except TimeoutException:
        logger.error("Error: Could not find search boxes or job results list did not load.")
        logger.error(f"Current URL: {driver.current_url}")
        debug_filename = "search_failure_page.html"
        try:
            with open(debug_filename, 'w', encoding='utf-8') as f: f.write(driver.page_source)
            logger.info(f"Saved search failure page source to {debug_filename}")
        except Exception as save_err: logger.error(f"Could not save search failure page source: {save_err}")
        return False
    except Exception as e:
        logger.error(f"An error occurred during search input: {e}")
        return False

@task(name="Save Job HTML", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def save_job_html_task(driver_page_source: str, job_id: str, page_num: int, keywords: str, location: str, output_dir: str):
    """Saves the provided page source HTML for a specific job."""
    # Pass page_source directly to make task potentially faster if driver state isn't needed
    logger = get_run_logger()
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        safe_keywords = _clean_filename(keywords[:30])
        safe_location = _clean_filename(location[:20])
        filename = f"linkedin_{safe_keywords}_{safe_location}_page_{page_num:02d}_job_{job_id}.html"
        filepath = output_path / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(driver_page_source)
        # logger.debug(f"Saved HTML to {filepath}") # Log only on debug

    except Exception as e:
        logger.error(f"Error saving HTML for job {job_id} to {filename}: {e}")


@task(name="Navigate Next Page", retries=1, retry_delay_seconds=5, cache_policy=NO_CACHE)
def navigate_next_page_task(driver: webdriver.Chrome, current_page_for_display: int, timeout: int, interaction_delay: float) -> bool:
    """Attempts to click the 'Next' pagination button."""
    logger = get_run_logger()
    logger.info("Attempting to navigate to the next page...")
    next_button = None
    try:
        next_button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(PAGINATION_NEXT_BUTTON_SELECTOR)
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
        time.sleep(0.5)
        next_button.click()
        logger.info(f"Clicked 'Next'. Waiting for page {current_page_for_display + 1} to load...")
        time.sleep(interaction_delay * 1.5)
        # Add more robust wait here if needed
        return True

    except (NoSuchElementException, TimeoutException):
        logger.warning("Could not find or click the 'Next' button. Assuming end of results.")
        return False
    except ElementClickInterceptedException:
         logger.warning("Click intercepted for 'Next' button. Trying JS click.")
         time.sleep(1)
         try:
             next_button = driver.find_element(*PAGINATION_NEXT_BUTTON_SELECTOR) # Re-find
             driver.execute_script("arguments[0].click();", next_button)
             logger.info(f"Clicked 'Next' via JS. Waiting for page {current_page_for_display + 1}...")
             time.sleep(interaction_delay * 1.5)
             return True
         except Exception as e_click:
             logger.error(f"Failed to click 'Next' button via JS after intercept: {e_click}")
             return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during pagination: {e}")
        return False

@task(name="Close WebDriver", cache_policy=NO_CACHE)
def close_driver_task(driver: webdriver.Chrome):
    """Closes the WebDriver."""
    logger = get_run_logger()
    if driver:
        try:
            logger.info("Closing WebDriver.")
            driver.quit()
        except Exception as e:
            logger.error(f"Error closing WebDriver: {e}")

# --- Helper Functions (used within the flow) ---

def scroll_down_job_list(driver: webdriver.Chrome, scroll_container_selector: str | None, pauses: int, delay: float):
    """Scrolls down the specific job list container (or window)."""
    logger = get_run_logger()
    logger.debug(f"Scrolling job list container '{scroll_container_selector}' {pauses} times...")
    scroll_element = None
    try:
        if scroll_container_selector:
            scroll_element = driver.find_element(By.CSS_SELECTOR, scroll_container_selector)
            scroll_script = "arguments[0].scrollTop = arguments[0].scrollHeight"
        else:
            logger.debug("No scroll container specified, scrolling window.")
            scroll_script = "window.scrollTo(0, document.body.scrollHeight);"

        for i in range(pauses):
            if scroll_element:
                driver.execute_script(scroll_script, scroll_element)
            else:
                driver.execute_script(scroll_script)
            # logger.debug(f"Scroll {i+1}/{pauses} executed.")
            time.sleep(delay)
        logger.debug("Scrolling job list finished.")
    except NoSuchElementException:
        logger.warning(f"Could not find scroll container '{scroll_container_selector}'. Scrolling window.")
        scroll_script = "window.scrollTo(0, document.body.scrollHeight);"
        for i in range(pauses):
            driver.execute_script(scroll_script)
            time.sleep(delay)
    except Exception as e:
        logger.error(f"Error during scrolling: {e}")
    time.sleep(1.0) # Pause after scrolling

def get_current_page_number(driver: webdriver.Chrome) -> int | None:
    """Attempts to find the current active page number from pagination."""
    try:
        page_element = driver.find_element(*CURRENT_PAGE_SELECTOR)
        return int(page_element.text.strip())
    except (NoSuchElementException, ValueError, StaleElementReferenceException):
        return None

# --- Prefect Flow ---

@flow(name="LinkedIn Job Scraper Flow")
def linkedin_scrape_flow(
    # Parameters now read from environment in __main__ and passed in
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
    Scrapes LinkedIn job postings based on keywords and location,
    saving the HTML source of each job details page.
    Reads configuration from environment variables loaded via .env file.
    """
    logger = get_run_logger()
    driver = None
    total_jobs_saved = 0

    # Log key parameters being used (avoid logging password directly)
    logger.info(f"Starting scrape for keywords: '{search_keywords}' in location: '{location}'")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Max pages: {max_pages_to_scrape}, Timeout: {page_load_timeout}, Interaction Delay: {interaction_delay}")
    if li_at_cookie:
        logger.info("Using li_at cookie for login.")
    elif linkedin_email:
        logger.info(f"Using email ({linkedin_email}) for login.") # Log email but not password
    else:
        logger.warning("No valid login method configured (Cookie or Email/Password).")


    try:
        driver_future = setup_driver.submit() # Submit async
        driver = driver_future.result() # Wait for driver setup

        if not driver: # Check if setup failed
             raise Exception("WebDriver setup failed, cannot continue.")

        login_successful = login_task.submit(
            driver, linkedin_email, linkedin_password, li_at_cookie,
            page_load_timeout, interaction_delay
        ).result() # Block until login completes

        if not login_successful:
            logger.error("Login failed. Aborting flow.")
            return # Stop the flow

        search_successful = search_jobs_task.submit(
            driver, search_keywords, location, page_load_timeout, interaction_delay
        ).result() # Block until search completes

        if not search_successful:
            logger.error("Initial job search failed. Aborting flow.")
            return # Stop the flow

        # --- Loop Through Pages ---
        current_page_num_for_loop = 1
        processed_job_ids_global = set()

        while current_page_num_for_loop <= max_pages_to_scrape:
            actual_page_num = get_current_page_number(driver)
            display_page_num = actual_page_num if actual_page_num is not None else current_page_num_for_loop
            logger.info(f"--- Processing Page {display_page_num} (Attempt {current_page_num_for_loop}) ---")

            processed_job_ids_on_page = set()
            new_jobs_found_in_scroll_cycle = True
            scroll_attempt = 0

            while scroll_attempt <= scroll_pauses_within_page:
                if scroll_attempt > 0 and not new_jobs_found_in_scroll_cycle:
                    logger.debug(f"No new jobs found in previous scroll pass on page {display_page_num}. Stopping scrolls.")
                    break

                new_jobs_found_in_scroll_cycle = False
                job_cards = []
                try:
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located(JOB_LIST_SELECTOR))
                    job_cards = driver.find_elements(*JOB_CARD_SELECTOR)
                    logger.debug(f"Found {len(job_cards)} potential job cards on screen (Scroll attempt {scroll_attempt+1}).")
                except (NoSuchElementException, TimeoutException):
                    logger.warning("Job card list selector not found or timed out after scroll.")
                    break

                jobs_to_process_this_pass = []
                for card in job_cards:
                    job_id = None
                    try:
                        job_id = card.get_attribute('data-job-id')
                        if job_id and job_id not in processed_job_ids_global:
                            jobs_to_process_this_pass.append(job_id)
                            processed_job_ids_on_page.add(job_id)
                            processed_job_ids_global.add(job_id) # Mark globally processed
                            new_jobs_found_in_scroll_cycle = True
                    except StaleElementReferenceException:
                        logger.warning(f"Stale element ref checking job card ID {job_id or '(unknown)'}. Skipping.")
                        continue

                if jobs_to_process_this_pass:
                     logger.info(f"Found {len(jobs_to_process_this_pass)} new job(s) to process in this pass.")

                # Click each newly identified job and save HTML
                save_tasks = [] # Collect save tasks if running them async
                for i, job_id in enumerate(jobs_to_process_this_pass):
                    logger.info(f"  Processing job {i+1}/{len(jobs_to_process_this_pass)} (ID: {job_id}) on page {display_page_num}")
                    try:
                        job_element = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, f"div[data-job-id='{job_id}']"))
                        )
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest', behavior: 'smooth'});", job_element)
                        time.sleep(0.8 + interaction_delay * 0.2)

                        try:
                            job_element.click()
                        except ElementClickInterceptedException:
                            logger.warning(f"  Direct click intercepted for job ID: {job_id}. Trying JS click.")
                            driver.execute_script("arguments[0].click();", job_element)

                        time.sleep(0.3)
                        WebDriverWait(driver, page_load_timeout).until(EC.visibility_of_element_located(JOB_DETAIL_TITLE_SELECTOR))
                        time.sleep(interaction_delay * 0.75) # Wait for dynamic content

                        # Get page source *after* waiting
                        current_page_source = driver.page_source

                        # Submit save task
                        # Pass page source instead of driver object
                        task_run = save_job_html_task.submit(
                            current_page_source, job_id, display_page_num,
                            search_keywords, location, output_dir
                         )
                        save_tasks.append(task_run) # Optional: track tasks
                        total_jobs_saved += 1

                    except TimeoutException:
                        logger.error(f"  Error: Timeout waiting for job element (ID: {job_id}), details, or title.")
                    except ElementClickInterceptedException:
                        logger.error(f"  Error: Click still intercepted for job ID: {job_id} after retry. Skipping.")
                    except StaleElementReferenceException:
                         logger.error(f"  Error: Job element (ID: {job_id}) became stale. Skipping.")
                    except Exception as e:
                        logger.error(f"  Error processing job ID {job_id}: {e}")

                # Scroll down
                if scroll_attempt < scroll_pauses_within_page :
                    logger.debug(f"Scrolling down page/list (Attempt {scroll_attempt + 1}/{scroll_pauses_within_page})")
                    scroll_down_job_list(driver, ".jobs-search-results-list__list", pauses=1, delay=delay_between_scrolls)
                    scroll_attempt += 1
                else:
                    logger.debug("Max scroll attempts reached for this page.")
                    break # Exit scroll loop

            logger.info(f"Finished processing page {display_page_num}. Processed {len(processed_job_ids_on_page)} unique jobs found on this page.")

            # --- Go to Next Page ---
            if current_page_num_for_loop >= max_pages_to_scrape:
                logger.info(f"Reached maximum page limit ({max_pages_to_scrape}).")
                break

            navigation_successful = navigate_next_page_task.submit(
                driver, display_page_num, page_load_timeout, interaction_delay
            ).result() # Block until navigation attempt completes

            if not navigation_successful:
                logger.info("Could not navigate to the next page. Ending scraping.")
                break

            current_page_num_for_loop += 1
            time.sleep(interaction_delay / 2)

        logger.info(f"\n--- Scraping Finished ---")
        logger.info(f"Processed {current_page_num_for_loop -1} pages (or reached end/limit).")
        logger.info(f"Attempted to save HTML for {total_jobs_saved} jobs.")
        logger.info(f"Saved files located in: {Path(output_dir).resolve()}")

    except Exception as e:
        logger.error(f"\nAn critical error occurred in the flow: {e}", exc_info=True)

    finally:
        if driver:
            close_driver_task.submit(driver)
            logger.info("Submitted WebDriver close task.")

# linkedin_actions.py
"""Prefect tasks for LinkedIn interactions (login, search, save, navigate) and helpers."""

import time
import re
from pathlib import Path
from selenium.webdriver.remote.webdriver import WebDriver # Use specific type hint
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
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE

# Import constants from config file
import config

# --- Helper Function (Internal Use) ---
def _clean_filename(text):
    """Removes characters unsafe for filenames."""
    # Keep text relatively short for filenames
    text = text[:50]
    text = re.sub(r'[\\/*?:"<>|\'.]', "", text) # Added dot and single quote
    text = re.sub(r'\s+', '_', text) # Replace whitespace with underscore
    return text.strip('_')


# --- Prefect Tasks ---

@task(name="Login to LinkedIn", retries=1, retry_delay_seconds=5, cache_policy=NO_CACHE)
def login_task(driver: WebDriver, email: str | None, password: str | None, cookie: str | None, timeout: int, interaction_delay: float) -> bool:
    """Logs into LinkedIn using email/password or a cookie."""
    logger = get_run_logger()

    # Prioritize cookie if provided
    if cookie:
        logger.info("Attempting login with li_at cookie...")
        # Go to a non-feed page first to set cookie reliably
        driver.get(config.JOBS_URL) # Go to jobs page first
        time.sleep(1)
        if not driver.current_url.startswith("https://www.linkedin.com"):
             logger.warning(f"Current URL {driver.current_url} not on linkedin.com, potential issue setting cookie.")
             driver.get("https://www.linkedin.com") # Fallback to base domain
             time.sleep(2)

        try:
            driver.delete_all_cookies() # Start fresh
            driver.add_cookie({
                "name": "li_at", "value": cookie,
                "domain": ".linkedin.com", "path": "/",
                "secure": True, # Usually required
                "httpOnly": True # Usually required
            })
            logger.info("li_at cookie added.")
            driver.get("https://www.linkedin.com/feed/") # Verify by going to feed
            time.sleep(interaction_delay)
        except Exception as e:
             logger.error(f"Failed to add cookie or navigate after adding: {e}")
             # Try navigating again just in case
             try:
                 driver.get("https://www.linkedin.com/feed/")
                 time.sleep(interaction_delay)
             except Exception as e2:
                 logger.error(f"Second navigation attempt after cookie error failed: {e2}")
                 return False

    # Fallback to email/password if cookie is not provided or empty
    elif email and password:
        logger.info("Attempting login with email/password...")
        driver.get(config.LOGIN_URL)
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.ID, config.USERNAME_FIELD_ID))
            )
            logger.debug("Login page loaded.")
            email_elem = driver.find_element(By.ID, config.USERNAME_FIELD_ID)
            email_elem.send_keys(email)
            time.sleep(0.5 + interaction_delay * 0.1) # Small dynamic delay
            password_elem = driver.find_element(By.ID, config.PASSWORD_FIELD_ID)
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
            EC.presence_of_element_located(config.VERIFY_LOGIN_SELECTOR)
        )
         # Additional check: Make sure we are not on a challenge/security page
        current_url = driver.current_url
        if "checkpoint" in current_url or "challenge" in current_url or "login" in current_url:
            logger.error(f"Login verification failed. Ended up on URL: {current_url}")
            # Save page source might be useful here too
            return False
        logger.info("Login successful!")
        return True
    except TimeoutException:
        logger.error(f"Login verification failed. Element {config.VERIFY_LOGIN_SELECTOR} not found.")
        current_url = driver.current_url
        logger.error(f"Current URL: {current_url}")
        if "checkpoint" in current_url or "challenge" in current_url:
            logger.error("LinkedIn is asking for verification (CAPTCHA, phone, etc.). Manual intervention required or script needs adjustment.")
        elif "session_password" in current_url or "login" in current_url:
             logger.error("Incorrect password or login issue suspected.")

        # Attempt to save page source for debugging
        debug_filename = "login_failure_page.html"
        try:
            with open(debug_filename, 'w', encoding='utf-8') as f: f.write(driver.page_source)
            logger.info(f"Saved login failure page source to {debug_filename}")
        except Exception as save_err: logger.error(f"Could not save login failure page source: {save_err}")
        return False


@task(name="Search Jobs", cache_policy=NO_CACHE)
def search_jobs_task(driver: WebDriver, keywords: str, location: str, timeout: int, interaction_delay: float) -> bool:
    """Navigates to jobs page and performs search."""
    logger = get_run_logger()
    logger.info(f"Navigating to LinkedIn Jobs: {config.JOBS_URL}")
    driver.get(config.JOBS_URL)
    time.sleep(interaction_delay * 0.5) # Wait for page elements to start rendering

    try:
        logger.info(f"Searching for Keywords: '{keywords}'")
        kw_input = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(config.KEYWORD_SEARCH_SELECTOR))
        # Clear keyword input robustly
        kw_input.clear() # Try standard clear first
        time.sleep(0.2)
        if kw_input.get_attribute('value'): # If not empty, use JS
            driver.execute_script("arguments[0].value = '';", kw_input)
            time.sleep(0.2)
        kw_input.send_keys(keywords)
        time.sleep(0.7)

        logger.info(f"Searching for Location: '{location}'")
        loc_input = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(config.LOCATION_SEARCH_SELECTOR))
        # Clear location more carefully - click, select all, delete, then send keys
        loc_input.click()
        time.sleep(0.3)
        loc_input.send_keys(Keys.CONTROL + "a") # Select all
        time.sleep(0.2)
        loc_input.send_keys(Keys.DELETE)
        time.sleep(0.3)
        # Ensure it's empty before sending keys
        if loc_input.get_attribute('value'):
            logger.warning("Location field clear failed, trying JS.")
            driver.execute_script("arguments[0].value = '';", loc_input)
            time.sleep(0.3)

        loc_input.send_keys(location)
        time.sleep(1.2) # Allow suggestions to appear/disappear
        loc_input.send_keys(Keys.RETURN)
        logger.info("Search submitted.")
        # Wait for results list to appear as confirmation
        time.sleep(interaction_delay) # Extra pause for full load
        return True

    except TimeoutException as e:
        logger.error(f"Error: Timeout during search setup or results loading: {e}")
        logger.error(f"Current URL: {driver.current_url}")
        debug_filename = "search_failure_page.html"
        try:
            with open(debug_filename, 'w', encoding='utf-8') as f: f.write(driver.page_source)
            logger.info(f"Saved search failure page source to {debug_filename}")
        except Exception as save_err: logger.error(f"Could not save search failure page source: {save_err}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during search input: {e}")
        return False


@task(name="Save Job HTML", retries=2, retry_delay_seconds=5, cache_policy=NO_CACHE)
def save_job_html_task(driver_page_source: str, job_id: str, page_num: int, keywords: str, location: str, output_dir: str):
    """Saves the provided page source HTML for a specific job."""
    logger = get_run_logger()
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        safe_keywords = _clean_filename(keywords)
        safe_location = _clean_filename(location)
        # Format filename consistently
        filename = f"linkedin_{safe_keywords}_{safe_location}_p{page_num:02d}_id{job_id}.html"
        filepath = output_path / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(driver_page_source)
        logger.debug(f"Saved HTML to {filepath}") # Log only on debug

    except Exception as e:
        logger.error(f"Error saving HTML for job {job_id} to {filename}: {e}")


@task(name="Navigate Next Page", retries=1, retry_delay_seconds=5, cache_policy=NO_CACHE)
def navigate_next_page_task(driver: WebDriver, current_page_for_display: int, timeout: int, interaction_delay: float) -> bool:
    """Attempts to click the 'Next' pagination button."""
    logger = get_run_logger()
    logger.info("Attempting to navigate to the next page...")
    next_button = None
    try:
        # Ensure pagination exists and find the button
        WebDriverWait(driver, 5).until(EC.presence_of_element_located(config.PAGINATION_NEXT_BUTTON_SELECTOR))
        next_button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(config.PAGINATION_NEXT_BUTTON_SELECTOR)
        )
        # Scroll button into view before clicking
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
        time.sleep(0.5 + interaction_delay * 0.1)

        # Record current page number *before* click (if possible)
        current_page_num_before_click = get_current_page_number(driver)

        next_button.click()
        logger.info(f"Clicked 'Next'. Waiting for page {current_page_for_display + 1} to load...")
        time.sleep(interaction_delay * 0.5) # Short wait after click

        # Wait for page number to change or for list to update
        try:
             WebDriverWait(driver, timeout).until(
                 lambda d: get_current_page_number(d) != current_page_num_before_click and get_current_page_number(d) == current_page_for_display + 1
             )
             logger.info(f"Detected page number change to {get_current_page_number(driver)}.")
        except TimeoutException:
            logger.warning(f"Page number did not update as expected after clicking next. Proceeding based on time delay.")
            time.sleep(interaction_delay * 1.5) # Fallback to longer delay

        # Extra check: Wait briefly for job list to potentially reload
        try:
            WebDriverWait(driver, 5).until(EC.staleness_of(driver.find_element(*config.JOB_LIST_SELECTOR)))
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located(config.JOB_LIST_SELECTOR))
            logger.debug("Job list seems to have refreshed.")
        except: # Ignore errors here, it's just a best-effort check
            logger.debug("Staleness check for job list passed or timed out.")
            pass
        time.sleep(interaction_delay * 0.5) # Final short pause
        return True

    except (NoSuchElementException, TimeoutException):
        logger.warning("Could not find or click the 'Next' button. Assuming end of results.")
        return False
    except ElementClickInterceptedException:
         logger.warning("Click intercepted for 'Next' button. Trying JS click.")
         time.sleep(1)
         try:
             # Re-find the element before JS click
             next_button = driver.find_element(*config.PAGINATION_NEXT_BUTTON_SELECTOR)
             driver.execute_script("arguments[0].click();", next_button)
             logger.info(f"Clicked 'Next' via JS. Waiting for page {current_page_for_display + 1}...")
             # Use same wait logic as above after JS click
             current_page_num_before_click = get_current_page_number(driver) # Re-check page number
             time.sleep(interaction_delay * 0.5)
             try:
                 WebDriverWait(driver, timeout).until(
                     lambda d: get_current_page_number(d) != current_page_num_before_click and get_current_page_number(d) == current_page_for_display + 1
                 )
                 logger.info(f"Detected page number change to {get_current_page_number(driver)} after JS click.")
             except TimeoutException:
                 logger.warning(f"Page number did not update as expected after JS click. Proceeding based on time delay.")
                 time.sleep(interaction_delay * 1.5)
             time.sleep(interaction_delay * 0.5)
             return True
         except Exception as e_click:
             logger.error(f"Failed to click 'Next' button via JS after intercept: {e_click}")
             return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during pagination: {e}")
        return False


# --- Helper Functions (Used by Flow) ---

def scroll_down_job_list(driver: WebDriver, scroll_container_selector: str | None, pauses: int, delay: float):
    """Scrolls down the specific job list container (or window)."""
    logger = get_run_logger()
    logger.debug(f"Scrolling job list {pauses} times...")
    scroll_element = None
    use_window_scroll = False

    if scroll_container_selector:
        try:
            scroll_element = driver.find_element(By.CSS_SELECTOR, scroll_container_selector)
            scroll_script = "arguments[0].scrollTop = arguments[0].scrollHeight"
            logger.debug(f"Using scroll container: {scroll_container_selector}")
        except NoSuchElementException:
            logger.warning(f"Could not find scroll container '{scroll_container_selector}'. Defaulting to window scroll.")
            use_window_scroll = True
    else:
         use_window_scroll = True

    if use_window_scroll:
        logger.debug("Using window scroll.")
        scroll_script = "window.scrollTo(0, document.body.scrollHeight);"

    last_scroll_height = -1 # Initialize
    for i in range(pauses):
        try:
            if scroll_element and not use_window_scroll:
                 current_height = driver.execute_script("return arguments[0].scrollHeight", scroll_element)
                 driver.execute_script(scroll_script, scroll_element)
            else:
                 current_height = driver.execute_script("return document.body.scrollHeight")
                 driver.execute_script(scroll_script)

            time.sleep(delay) # Wait for content to load after scroll action

            # Check if scroll height changed - break if it didn't after a pause
            if i > 0: # Don't check on the first scroll
                new_height = driver.execute_script("return arguments[0].scrollHeight", scroll_element) if scroll_element and not use_window_scroll else driver.execute_script("return document.body.scrollHeight")
                if new_height == current_height:
                    logger.debug(f"Scroll height did not change after scroll {i+1}. Stopping scroll attempts.")
                    break
                last_scroll_height = new_height

        except Exception as e:
            logger.error(f"Error during scroll {i+1}/{pauses}: {e}")
            break # Stop scrolling on error

    logger.debug("Scrolling job list finished.")
    time.sleep(1.0) # Pause after all scrolling


def get_current_page_number(driver: WebDriver) -> int | None:
    """Attempts to find the current active page number from pagination."""
    try:
        page_element = WebDriverWait(driver, 2).until(
            EC.visibility_of_element_located(config.CURRENT_PAGE_SELECTOR)
        )
        # page_element = driver.find_element(*config.CURRENT_PAGE_SELECTOR)
        return int(page_element.text.strip())
    except (NoSuchElementException, ValueError, TimeoutException, StaleElementReferenceException):
        # get_run_logger().debug("Could not find or parse current page number.", exc_info=True) # Optional debug
        return None
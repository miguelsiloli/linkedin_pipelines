# webdriver_utils.py
"""WebDriver setup and teardown utilities."""

import os, shutil, tempfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService # Use 'Service' for clarity
from selenium.webdriver.chrome.options import Options as ChromeOptions # Use 'Options' for clarity
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE

@task(name="Setup WebDriver", retries=2, retry_delay_seconds=10, cache_policy=NO_CACHE) # Added retries
def setup_driver() -> webdriver.Chrome: # Or ChromeWebDriver
    """Initializes and returns a Chrome WebDriver instance, configured for CI."""
    logger = get_run_logger()
    logger.info("Setting up WebDriver for CI environment...")
    
    temp_user_data_dir_path_str = None # Initialize for cleanup logic
    driver_instance = None # Initialize for cleanup logic

    try:
        options = ChromeOptions() # Use the imported alias

        # --- ESSENTIAL CI/HEADLESS OPTIONS ---
        # options.add_argument("--headless=new") # Modern headless mode
        options.add_argument("--no-sandbox") # CRITICAL for CI
        options.add_argument("--disable-dev-shm-usage") # CRITICAL for CI
        # options.add_argument("--disable-gpu") # Recommended for headless
        options.add_argument("--window-size=1920,1080") # Set a consistent window size
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        # options.add_argument("--remote-debugging-port=9222") # Optional, can help sometimes
        options.add_argument("--disable-setuid-sandbox") # Additional sandbox flag

        # --- UNIQUE USER DATA DIRECTORY (THE FIX FOR YOUR ERROR) ---
        temp_dir = tempfile.mkdtemp()
        temp_user_data_dir_path_str = str(temp_dir)
        options.add_argument(f"--user-data-dir={temp_user_data_dir_path_str}")
        logger.info(f"Using temporary user data directory: {temp_user_data_dir_path_str}")
        # --- END OF UNIQUE USER DATA DIRECTORY ---

        # Your existing options (some might be redundant or less critical in headless CI)
        # options.add_argument("--start-maximized") # Usually not effective in headless, window-size is better
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument("--lang=en-US")
        
        # Options to make Selenium less detectable (good to keep)
        options.add_experimental_option('excludeSwitches', ['enable-logging']) # Keeps logs cleaner
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Log all arguments being passed to Chrome for debugging
        logger.info(f"Final Chrome options arguments: {options.arguments}")

        service = ChromeService(executable_path=ChromeDriverManager().install(), log_output=os.devnull)
        
        driver_instance = webdriver.Chrome(service=service, options=options)
        
        # Set implicit wait *after* driver instantiation
        driver_instance.implicitly_wait(5) # Increased slightly, 3 is very short

        # Attempt to hide the webdriver flag from navigator
        driver_instance.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Store the temp_user_data_dir path on the driver object for cleanup by the calling flow
        driver_instance.temp_user_data_dir = temp_user_data_dir_path_str
        temp_user_data_dir_path_str = None # Ownership of cleanup transferred to caller via driver object

        logger.info("WebDriver setup complete.")
        return driver_instance

    except Exception as e:
        logger.error(f"WebDriver setup failed: {e}", exc_info=True) # Log with traceback
        # If temp_user_data_dir_path_str was created but driver setup failed before ownership transfer
        if temp_user_data_dir_path_str:
            logger.info(f"Cleaning up failed setup's temporary user data directory: {temp_user_data_dir_path_str}")
            shutil.rmtree(temp_user_data_dir_path_str, ignore_errors=True)
        # If driver_instance was partially created but not returned
        if driver_instance:
            try:
                driver_instance.quit()
            except:
                pass # Ignore errors during cleanup of a failed setup
        raise # Re-raise the exception so Prefect knows the task failed

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
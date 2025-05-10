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
from pathlib import Path
from selenium.webdriver.chrome.service import Service

@task(name="Setup WebDriver")
def setup_driver(is_ci_environment: bool = True): # Pass a flag or detect CI
    logger = get_run_logger()
    options = webdriver.ChromeOptions()
    user_data_dir = tempfile.mkdtemp(prefix="chrome_profile_")
    logger.info(f"Using temporary user data directory: {user_data_dir}")

    options.add_argument(f"--user-data-dir={user_data_dir}")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-setuid-sandbox") # Redundant with --no-sandbox generally
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36") # Update agent
    options.add_argument("--lang=en-US")

    if is_ci_environment: # Check environment variable or pass as param
        logger.info("CI environment detected, enabling headless mode.")
        options.add_argument("--headless=new") # Crucial for CI
    # else:
        # logger.info("Local environment, running headful (or configure as needed).")


    # Store the user_data_dir path, e.g., on the options object if needed later,
    # or return it along with the driver.
    options.custom_user_data_dir = user_data_dir # Example of attaching for later cleanup

    driver_instance = None
    try:
        # Make sure ChromeDriver is correctly installed or use webdriver_manager
        service = Service(ChromeDriverManager().install())
        driver_instance = webdriver.Chrome(service=service, options=options)
        # Attach user_data_dir to driver for easier cleanup by close_driver_task
        driver_instance.user_data_dir_path = user_data_dir
        return driver_instance
    except Exception as e:
        logger.error(f"WebDriver setup failed: {e}", exc_info=True)
        # Attempt to clean up the created directory if session creation failed
        if Path(user_data_dir).exists():
            logger.info(f"Attempting to clean up user_data_dir {user_data_dir} after setup failure.")
            shutil.rmtree(user_data_dir, ignore_errors=True)
        raise # Re-raise the original exception to fail the task

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
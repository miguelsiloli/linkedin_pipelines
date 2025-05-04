# webdriver_utils.py
"""WebDriver setup and teardown utilities."""

import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from prefect import task, get_run_logger
from prefect.cache_policies import NO_CACHE

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
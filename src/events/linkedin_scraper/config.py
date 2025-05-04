# config.py
"""Configuration constants for the LinkedIn Scraper."""

from selenium.webdriver.common.by import By

# --- URLs ---
LOGIN_URL = "https://www.linkedin.com/login"
JOBS_URL = "https://www.linkedin.com/jobs/"

# --- Selectors ---
# Login
USERNAME_FIELD_ID = "username"
PASSWORD_FIELD_ID = "password"
VERIFY_LOGIN_SELECTOR = (By.ID, "global-nav")

# Job Search
KEYWORD_SEARCH_SELECTOR = (By.CSS_SELECTOR, "input[id^='jobs-search-box-keyword-id']")
LOCATION_SEARCH_SELECTOR = (By.CSS_SELECTOR, "input[id^='jobs-search-box-location-id']")

# Job Results & Details
JOB_LIST_SELECTOR = (By.CSS_SELECTOR, "div.scaffold-layout__list")
JOB_CARD_SELECTOR = (By.CSS_SELECTOR, "div.job-card-container[data-job-id]") 
JOB_DETAIL_PANE_SELECTOR = (By.CSS_SELECTOR, "div.jobs-search__job-details--container") # Keep if needed elsewhere
JOB_DETAIL_TITLE_SELECTOR = (By.CSS_SELECTOR, ".jobs-details-top-card__job-title")
JOB_LIST_SCROLL_CONTAINER = ".jobs-search-results-list__list" # Specific scroll container

# Pagination
PAGINATION_NEXT_BUTTON_SELECTOR = (By.CSS_SELECTOR, "button[aria-label='View next page']")
CURRENT_PAGE_SELECTOR = (By.CSS_SELECTOR, "li[data-test-pagination-page-btn].active > span")
import os
import time
import logging
import datetime
import pandas as pd
import zipfile
import re
import random
import undetected_chromedriver as uc
from selenium_stealth import stealth

from config import (
    BASE_URL, BROWSER, HEADLESS, USE_STEALTH, IMPLICIT_WAIT, EXPLICIT_WAIT_TIMEOUT, DOWNLOAD_WAIT_TIMEOUT,
    DOWNLOAD_DIR, LOG_DIR, LOG_FORMAT, DATE_FORMAT, MAX_RETRIES, RETRY_DELAY,
    NAV_FUND_TAB, NAV_FUND_COMPOSITION_MENU,
    SELECT_FUND_TYPE, SELECT_REGION, SELECT_FUND_UNIVERSE, INPUT_START_DATE, INPUT_END_DATE,
    BUTTON_SEARCH, BUTTON_EXCEL_DOWNLOAD, DATASET_CONFIGS,
    KOREAN_TO_ENGLISH_CATEGORY, KOREAN_TO_ENGLISH_METRIC, TIMESERIES_CODES,
    get_chrome_options, get_stealth_chrome_options, get_firefox_options
)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException,
    WebDriverException
)

# Set up logging
current_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = os.path.join(LOG_DIR, f"kofia_scraper_{current_timestamp}.log")
logging.basicConfig(filename=log_file_path, level=logging.INFO,
                    format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger(__name__)

class KOFIAScraper:
    """
    A robust Selenium web scraper for the KOFIA website to extract monthly
    financial data about Korean mutual funds.
    """

    def __init__(self, use_fixed_date=False):
        """
        Initializes the scraper with WebDriver and configurations.

        Args:
            use_fixed_date (bool): If True, sets the end date filter to 2025-03-31 for initial release.
                                   Otherwise, uses the current date.
        """
        self.driver = self._initialize_driver()
        self.wait = WebDriverWait(self.driver, EXPLICIT_WAIT_TIMEOUT)
        self.downloaded_files = []
        self.all_processed_data = []
        self.timeseries_counter = 1
        self.use_fixed_date = use_fixed_date
        logger.info("KOFIAScraper initialized.")
        logger.info(f"Downloading to: {DOWNLOAD_DIR}")
        logger.info(f"Logging to: {log_file_path}")

    def _initialize_driver(self):
        """
        Initializes and returns the Selenium WebDriver based on the BROWSER config.
        Uses undetected-chromedriver for stealth mode if USE_STEALTH is True.
        """
        try:
            if BROWSER == "chrome":
                if USE_STEALTH:
                    # Use undetected-chromedriver for stealth mode
                    logger.info("Initializing Chrome in STEALTH mode (undetected-chromedriver)")
                    options = get_stealth_chrome_options()

                    # Try to initialize with explicit version first
                    try:
                        driver = uc.Chrome(
                            options=options,
                            use_subprocess=True,
                            version_main=139  # Explicit Chrome version
                        )
                        logger.info("✓ Driver initialized with version 139")
                    except Exception as e:
                        logger.warning(f"Version 139 failed: {e}. Trying auto-detect...")
                        try:
                            driver = uc.Chrome(
                                options=options,
                                use_subprocess=True,
                                version_main=None
                            )
                        except Exception as e2:
                            logger.error(f"Stealth mode failed: {e2}. Falling back to regular Chrome...")
                            options = get_chrome_options()
                            driver = webdriver.Chrome(options=options)

                    # Apply selenium-stealth
                    if isinstance(driver, uc.Chrome):
                        try:
                            logger.info("Applying selenium-stealth configuration...")
                            stealth(driver,
                                languages=["en-US", "en", "ko-KR", "ko"],
                                vendor="Google Inc.",
                                platform="Win32",
                                webgl_vendor="Intel Inc.",
                                renderer="Intel Iris OpenGL Engine",
                                fix_hairline=True,
                            )
                            logger.info("✓ Selenium-stealth applied")
                        except Exception as e:
                            logger.warning(f"Could not apply selenium-stealth: {e}")

                    # Apply additional anti-detection measures
                    if hasattr(driver, 'execute_script'):
                        try:
                            driver.execute_script("""
                                Object.defineProperty(navigator, 'webdriver', {
                                    get: () => undefined
                                });
                                Object.defineProperty(navigator, 'plugins', {
                                    get: () => [1, 2, 3, 4, 5]
                                });
                                window.chrome = {
                                    runtime: {},
                                    loadTimes: function() {},
                                    csi: function() {},
                                    app: {}
                                };
                            """)
                        except Exception as e:
                            logger.warning(f"Could not apply JS anti-detection: {e}")
                else:
                    # Regular Selenium WebDriver with basic stealth
                    logger.info("Initializing Chrome in standard mode")
                    options = get_chrome_options()
                    driver = webdriver.Chrome(options=options)

            elif BROWSER == "firefox":
                options = get_firefox_options()
                driver = webdriver.Firefox(options=options)
            else:
                raise ValueError(f"Unsupported browser: {BROWSER}")

            driver.implicitly_wait(IMPLICIT_WAIT)
            driver.set_page_load_timeout(60)  # 60 second page load timeout

            logger.info(f"WebDriver for {BROWSER} initialized successfully (Stealth: {USE_STEALTH}).")
            return driver
        except WebDriverException as e:
            logger.error(f"Failed to initialize WebDriver for {BROWSER}: {e}")
            raise

    def find_element_safely(self, locators, timeout=EXPLICIT_WAIT_TIMEOUT, screenshot_name=None):
        """
        Attempts to find an element using multiple locator strategies with retry logic.

        Args:
            locators (list): A list of tuples, where each tuple is (By.STRATEGY, "locator_value").
            timeout (int): Maximum time to wait for the element.
            screenshot_name (str): Name for the screenshot if an error occurs.

        Returns:
            WebElement: The found web element.

        Raises:
            NoSuchElementException: If the element cannot be found after all retries.
        """
        for attempt in range(MAX_RETRIES):
            for by_strategy, locator_value in locators:
                try:
                    logger.debug(f"Attempt {attempt + 1}: Trying to find element by {by_strategy} with value '{locator_value}'")
                    element = self.wait.until(EC.presence_of_element_located((by_strategy, locator_value)))
                    logger.info(f"Element found: {locator_value}")
                    return element
                except (NoSuchElementException, TimeoutException, StaleElementReferenceException) as e:
                    logger.warning(f"Attempt {attempt + 1}: Locator '{locator_value}' failed: {e.__class__.__name__}. Trying next locator.")
            
            if attempt < MAX_RETRIES - 1:
                logger.info(f"All locators failed for current attempt. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY + random.uniform(0, 2)) # Randomize delay slightly
            else:
                error_msg = f"Failed to find element after {MAX_RETRIES} attempts using any provided locators."
                logger.error(error_msg)
                if screenshot_name:
                    self.capture_screenshot(screenshot_name)
                raise NoSuchElementException(error_msg)
        return None # Should not be reached

    def capture_screenshot(self, name="error_screenshot"):
        """
        Captures a screenshot of the current page.
        """
        screenshot_path = os.path.join(LOG_DIR, f"{name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.png")
        try:
            self.driver.save_screenshot(screenshot_path)
            logger.error(f"Screenshot captured: {screenshot_path}")
        except Exception as e:
            logger.error(f"Failed to capture screenshot: {e}")

    def ensure_main_frame(self):
        """
        Ensures we're in the 'main' frame. Call this if frame context might be lost.
        """
        try:
            # Try to find an element we know exists in main frame
            self.driver.find_element(By.XPATH, "//img[@alt='펀드']")
            logger.debug("Already in main frame")
        except:
            # Not in main frame, switch to it
            try:
                self.driver.switch_to.default_content()
                self.driver.switch_to.frame("main")
                logger.info("Re-switched to main frame")
            except Exception as e:
                logger.error(f"Failed to switch to main frame: {e}")

    def wait_for_download_completion(self, initial_files, timeout=DOWNLOAD_WAIT_TIMEOUT):
        """
        Waits for a new file to appear in the download directory.

        Args:
            initial_files (set): Set of files present in the download directory before download started.
            timeout (int): Maximum time to wait for the download to complete.

        Returns:
            str: The full path of the newly downloaded file.

        Raises:
            TimeoutException: If no new file appears within the timeout.
        """
        start_time = time.time()
        new_file_path = None
        logger.info("Waiting for file download to complete...")

        while time.time() - start_time < timeout:
            current_files = set(os.listdir(DOWNLOAD_DIR))
            new_files = current_files - initial_files

            for fname in new_files:
                if not fname.endswith(('.tmp', '.crdownload', '.part')):  # Ignore temporary download files
                    new_file_path = os.path.join(DOWNLOAD_DIR, fname)
                    # Ensure file is completely written (check size stability)
                    current_size = -1
                    for _ in range(5): # Check size 5 times over 1 second
                        time.sleep(0.2)
                        if not os.path.exists(new_file_path): # File might have been renamed or moved
                            break
                        new_size = os.path.getsize(new_file_path)
                        if current_size == new_size and new_size > 0:
                            logger.info(f"Download complete: {new_file_path}")
                            return new_file_path
                        current_size = new_size
                    if new_file_path and os.path.exists(new_file_path): # Check if file still exists after checks
                        logger.info(f"Download complete (size check passed): {new_file_path}")
                        return new_file_path
                
            time.sleep(1) # Check every second

        self.capture_screenshot("download_timeout")
        raise TimeoutException(f"Download did not complete within {timeout} seconds.")

    def _select_dropdown_option(self, locator, value, by_type="index"):
        """
        Helper to select an option from a dropdown by index or visible text.
        Adds a small delay after selection.
        """
        for attempt in range(MAX_RETRIES):
            try:
                dropdown_element = self.find_element_safely([locator], screenshot_name=f"dropdown_select_fail_{locator[1]}")
                select = Select(dropdown_element)
                if by_type == "index":
                    select.select_by_index(value)
                    logger.info(f"Selected index '{value}' from dropdown '{locator[1]}'")
                elif by_type == "text":
                    select.select_by_visible_text(value)
                    logger.info(f"Selected text '{value}' from dropdown '{locator[1]}'")
                time.sleep(random.uniform(2, 3)) # Explicit sleep after selection
                return True
            except (NoSuchElementException, TimeoutException, StaleElementReferenceException) as e:
                logger.warning(f"Attempt {attempt + 1}: Failed to select dropdown '{locator[1]}' with value '{value}'. Error: {e.__class__.__name__}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error(f"Unexpected error selecting dropdown '{locator[1]}' with value '{value}': {e}")
                self.capture_screenshot(f"unexpected_dropdown_error_{locator[1]}")
                raise
        logger.error(f"Failed to select dropdown '{locator[1]}' with value '{value}' after {MAX_RETRIES} attempts.")
        return False

    def navigate_to_fund_composition_query(self):
        """
        Navigates to the fund composition query page following the runbook steps.

        Step 2 (Runbook): Click on the "펀드" (Fund) tab highlighted in red
        Step 3 (Runbook): Click on "투자자산별현황" (Investment Asset Status) from the left sidebar menu
        """
        self.driver.get(BASE_URL)
        logger.info(f"Navigated to {BASE_URL}")

        # CRITICAL: Website uses FRAMES - switch to 'main' frame
        logger.info("Switching to 'main' frame...")
        try:
            self.driver.switch_to.frame("main")
            logger.info("✓ Successfully switched to main frame")
            time.sleep(random.uniform(2, 4))
        except Exception as e:
            logger.error(f"Failed to switch to main frame: {e}")
            self.capture_screenshot("frame_switch_failed")
            raise

        # Try to switch page to English
        logger.info("Attempting to translate page to English...")
        try:
            # Look for English language link/button
            english_locators = [
                ("xpath", "//a[contains(text(), 'ENGLISH') or contains(text(), 'English')]"),
                ("xpath", "//a[contains(@href, 'lang=en') or contains(@href, 'language=en')]"),
                ("xpath", "//img[@alt='ENGLISH' or @alt='English']"),
            ]

            for by_type, locator in english_locators:
                try:
                    english_btn = self.driver.find_element(by_type, locator)
                    english_btn.click()
                    logger.info("✓ Clicked English language button")
                    time.sleep(3)
                    break
                except:
                    continue
            else:
                logger.info("No English switcher found - site will remain in Korean")
        except Exception as e:
            logger.warning(f"Language switch attempt failed: {e}")

        logger.info("Page loaded, searching for Fund tab...")

        # Step 2 from Runbook: Click on Fund (펀드) tab - this is the tab highlighted in RED
        # NAV_FUND_TAB is already a list of multiple locator strategies
        fund_tab = self.find_element_safely(NAV_FUND_TAB, screenshot_name="fund_tab_not_found")
        fund_tab.click()
        logger.info("✓ Step 2 Complete: Clicked on 'Fund (펀드)' tab (red highlighted tab in runbook).")

        # Wait for page content to load after clicking Fund tab
        logger.info("Waiting for fund data page to load...")
        time.sleep(random.uniform(4, 6))

        # Capture screenshot to see what loaded
        self.capture_screenshot("after_fund_tab_page")

        # After clicking Fund tab, we see a summary dashboard
        # Need to find and click the link to get to the query/form page with dropdowns
        logger.info("Looking for fund composition query form link...")

        try:
            # Try to find link to the detailed query form
            query_link_locators = [
                ("xpath", "//a[contains(text(), '집합투자기구규모')]"),
                ("xpath", "//a[contains(text(), '투자자산별현황')]"),
                ("xpath", "//a[contains(@href, 'fundComp')]"),
            ]

            query_link = self.find_element_safely(query_link_locators, timeout=10, screenshot_name="query_link_not_found")
            query_link.click()
            logger.info("✓ Step 3: Clicked on fund composition query form link")
            time.sleep(random.uniform(4, 6))

        except NoSuchElementException:
            logger.info("Query form link not found - might already be on the form page or need different navigation")

        self.capture_screenshot("fund_query_form_page")
        logger.info("Fund composition query form page loaded")

    def download_dataset(self, config):
        """
        Configures the form, searches, and downloads a single dataset.
        """
        dataset_name = config['name']
        output_name = config['output_name']
        fund_type_index = config['fund_type_index']
        fund_type_korean = config['fund_type_korean']
        region_index = config['region_index']
        region_korean = config['region_korean']
        fund_universe_index = config['fund_universe_index']
        time_window_years = config['time_window_years']

        logger.info(f"--- Starting download for dataset: {dataset_name} ---")
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                # Ensure we're in the correct frame
                self.ensure_main_frame()

                # 1. Reset/refresh the query form (if necessary, or ensure clean state)
                # KOFIA does not seem to have a "reset" button on this specific page.
                # Re-selecting options should be enough to ensure clean state.
                # If there's a clear form button, add it here.
                logger.info(f"Attempt {attempt + 1}: Configuring form for {dataset_name}")

                # Set end date (current date or fixed date)
                if self.use_fixed_date:
                    end_date_str = "2025-03-31"
                    end_date = datetime.date(2025, 3, 31)
                else:
                    end_date = datetime.date.today()
                    end_date_str = end_date.strftime("%Y-%m-%d")

                start_date = end_date - datetime.timedelta(days=time_window_years * 365)
                start_date_str = start_date.strftime("%Y-%m-%d")

                # Clear and set start date
                start_date_input = self.find_element_safely([INPUT_START_DATE], screenshot_name=f"{dataset_name}_start_date_input_not_found")
                start_date_input.clear()
                start_date_input.send_keys(start_date_str)
                logger.info(f"Set start date to: {start_date_str}")

                # Clear and set end date
                end_date_input = self.find_element_safely([INPUT_END_DATE], screenshot_name=f"{dataset_name}_end_date_input_not_found")
                end_date_input.clear()
                end_date_input.send_keys(end_date_str)
                logger.info(f"Set end date to: {end_date_str}")
                time.sleep(random.uniform(0.5, 1.5)) # Short pause after date input

                # 2. Select Fund Type
                self._select_dropdown_option(SELECT_FUND_TYPE, fund_type_index, by_type="index")
                logger.info(f"Selected Fund Type (Korean: {fund_type_korean}) by index {fund_type_index}")

                # 3. Select Region
                self._select_dropdown_option(SELECT_REGION, region_index, by_type="index")
                logger.info(f"Selected Region (Korean: {region_korean}) by index {region_index}")

                # 4. Select Fund Universe (assuming 'Total' is index 0)
                self._select_dropdown_option(SELECT_FUND_UNIVERSE, fund_universe_index, by_type="index")
                logger.info(f"Selected Fund Universe by index {fund_universe_index}")

                # Get initial files in download directory
                initial_files = set(os.listdir(DOWNLOAD_DIR))

                # 5. Click Search button
                search_button = self.find_element_safely([BUTTON_SEARCH], screenshot_name=f"{dataset_name}_search_button_not_found")
                search_button.click()
                logger.info("Clicked Search button. Waiting for results...")
                time.sleep(random.uniform(3, 5)) # Wait for results to load

                # Wait for a table or specific result element to confirm results loaded.
                # This is a placeholder, adapt to actual page structure.
                self.wait.until(EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'grid') or contains(@class, 'data')]")))
                logger.info("Search results loaded.")

                # 6. Locate and click Excel download icon
                excel_button = self.find_element_safely([BUTTON_EXCEL_DOWNLOAD], screenshot_name=f"{dataset_name}_excel_button_not_found")
                excel_button.click()
                logger.info("Clicked Excel download button.")

                # 7. Wait for download to complete
                downloaded_file_path = self.wait_for_download_completion(initial_files)
                if downloaded_file_path:
                    # 8. Rename file
                    new_file_name = f"{output_name}.xls"
                    new_file_path = os.path.join(DOWNLOAD_DIR, new_file_name)
                    os.rename(downloaded_file_path, new_file_path)
                    self.downloaded_files.append(new_file_path)
                    logger.info(f"Successfully downloaded and renamed to: {new_file_path}")
                    success = True
                    break # Exit retry loop on success
                else:
                    raise TimeoutException("Download path was not returned, implying failure.")

            except (NoSuchElementException, TimeoutException, StaleElementReferenceException, WebDriverException) as e:
                logger.warning(f"Error during download of {dataset_name} (Attempt {attempt + 1}): {e}")
                self.capture_screenshot(f"{dataset_name}_error_{attempt+1}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Retrying download for {dataset_name} in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY + random.uniform(0, 2))
                else:
                    logger.error(f"Failed to download {dataset_name} after {MAX_RETRIES} attempts.")
            except Exception as e:
                logger.error(f"An unexpected error occurred during download of {dataset_name}: {e}", exc_info=True)
                self.capture_screenshot(f"{dataset_name}_unexpected_error")
                break # Don't retry for unexpected errors, as they might be persistent

        return success

    def process_excel_file(self, file_path, dataset_config):
        """
        Processes a single downloaded Excel file, maps columns, and formats dates.
        """
        logger.info(f"Processing file: {file_path}")
        output_name_prefix = dataset_config['output_name']
        try:
            # Read Excel file, skipping the first two header rows
            df = pd.read_excel(file_path, header=[2, 3], engine='openpyxl')

            # --- Step 5: Data Processing ---
            # Rename columns
            new_columns = []
            for col_level_0, col_level_1 in df.columns:
                if col_level_0 == '기준일': # Date column
                    new_columns.append('Date')
                else:
                    category = KOREAN_TO_ENGLISH_CATEGORY.get(col_level_0.strip(), col_level_0.strip())
                    metric = KOREAN_TO_ENGLISH_METRIC.get(col_level_1.strip(), col_level_1.strip())
                    col_name = f"{output_name_prefix}: {category}: {metric}"
                    new_columns.append(col_name)

                    # Generate Timeseries Codes
                    if col_name not in TIMESERIES_CODES:
                        TIMESERIES_CODES[col_name] = f"KMFK_{self.timeseries_counter:04d}"
                        self.timeseries_counter += 1

            df.columns = new_columns
            logger.info(f"Columns renamed for {output_name_prefix}.")

            # --- Step 6: Date Formatting ---
            # Assuming 'Date' column is the first one
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df['Date'] = df['Date'].dt.strftime('%Y-%m')
            logger.info("Dates formatted to YYYY-MM.")

            # Validate the processed DataFrame structure
            if 'Date' not in df.columns or df.empty:
                raise ValueError(f"Processed DataFrame from {file_path} is invalid or missing 'Date' column.")

            self.all_processed_data.append(df)
            logger.info(f"Successfully processed {file_path}")
            return df

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}", exc_info=True)
            self.capture_screenshot(f"processing_error_{output_name_prefix}")
            return None

    def generate_output_files(self):
        """
        Combines all processed data into DATA and METADATA files and creates a ZIP archive.
        """
        if not self.all_processed_data:
            logger.warning("No data was successfully processed to generate output files.")
            return

        current_date_str = datetime.datetime.now().strftime("%Y%m%d")
        data_filename = f"KMFK_DATA_{current_date_str}.xls"
        metadata_filename = f"KMFK_META_{current_date_str}.xls"
        zip_filename = f"KMFK_{current_date_str}.ZIP"

        data_filepath = os.path.join(DOWNLOAD_DIR, data_filename)
        metadata_filepath = os.path.join(DOWNLOAD_DIR, metadata_filename)
        zip_filepath = os.path.join(DOWNLOAD_DIR, zip_filename)

        logger.info("Generating final DATA and METADATA files...")

        # --- DATA File Generation ---
        try:
            # Merge all DataFrames on 'Date' column
            merged_df = self.all_processed_data[0]
            for df in self.all_processed_data[1:]:
                merged_df = pd.merge(merged_df, df, on='Date', how='outer')

            # Sort by date
            merged_df = merged_df.sort_values('Date')

            # Create rows for CODE and DESCRIPTION
            codes = ['Date'] + [TIMESERIES_CODES.get(col, col) for col in merged_df.columns if col != 'Date']
            descriptions = ['Date'] + [col for col in merged_df.columns if col != 'Date']

            # Create final DataFrame with CODE and DESCRIPTION rows
            final_df = pd.DataFrame([codes, descriptions], columns=merged_df.columns)
            final_df = pd.concat([final_df, merged_df], ignore_index=True)

            # Write to Excel
            final_df.to_excel(data_filepath, index=False, header=False, engine='openpyxl')
            logger.info(f"DATA file created: {data_filepath}")

        except Exception as e:
            logger.error(f"Error generating DATA file: {e}", exc_info=True)
            return

        # --- METADATA File Generation ---
        try:
            # Calculate next release date (end of next month)
            today = datetime.datetime.now()
            if today.month == 12:
                next_month = datetime.datetime(today.year + 1, 1, 1)
            else:
                next_month = datetime.datetime(today.year, today.month + 1, 1)

            # Get last day of next month
            if next_month.month == 12:
                last_day = datetime.datetime(next_month.year + 1, 1, 1) - datetime.timedelta(days=1)
            else:
                last_day = datetime.datetime(next_month.year, next_month.month + 1, 1) - datetime.timedelta(days=1)

            next_release_date = last_day.strftime("%Y-%m-%dT12:00:00")

            # Create metadata rows
            metadata_rows = []
            for col in merged_df.columns:
                if col != 'Date':
                    code = TIMESERIES_CODES.get(col, col)
                    metadata_rows.append({
                        'CODE': code,
                        'DESCRIPTION': col,
                        'FREQUENCY': 'Monthly',
                        'UNIT': 'KRW Million' if 'Amount' in col else 'Percentage',
                        'NEXT_RELEASE_DATE': next_release_date
                    })

            metadata_df = pd.DataFrame(metadata_rows)
            metadata_df.to_excel(metadata_filepath, index=False, engine='openpyxl')
            logger.info(f"METADATA file created: {metadata_filepath}")

        except Exception as e:
            logger.error(f"Error generating METADATA file: {e}", exc_info=True)
            return

        # --- ZIP Archive Creation ---
        try:
            with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(data_filepath, os.path.basename(data_filepath))
                zipf.write(metadata_filepath, os.path.basename(metadata_filepath))
            logger.info(f"ZIP archive created: {zip_filepath}")
            print(f"\n✅ SUCCESS! Files generated:")
            print(f"   - {data_filename}")
            print(f"   - {metadata_filename}")
            print(f"   - {zip_filename}")

        except Exception as e:
            logger.error(f"Error creating ZIP archive: {e}", exc_info=True)

    def run(self):
        """
        Main execution method that runs the entire scraping workflow.
        """
        try:
            logger.info("=" * 50)
            logger.info("Starting KOFIA Web Scraper")
            logger.info("=" * 50)

            # Navigate to the fund composition query page
            self.navigate_to_fund_composition_query()

            # Download all datasets
            success_count = 0
            failure_count = 0

            for config in DATASET_CONFIGS:
                if self.download_dataset(config):
                    success_count += 1
                else:
                    failure_count += 1

            logger.info(f"\nDownload Summary: {success_count} successful, {failure_count} failed out of {len(DATASET_CONFIGS)} total")

            # Process all downloaded files
            for file_path, config in zip(self.downloaded_files, DATASET_CONFIGS):
                self.process_excel_file(file_path, config)

            # Generate final output files
            self.generate_output_files()

            logger.info("=" * 50)
            logger.info("KOFIA Web Scraper completed successfully")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"Fatal error in scraper execution: {e}", exc_info=True)
            self.capture_screenshot("fatal_error")

        finally:
            # Clean up
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='KOFIA Mutual Fund Data Scraper')
    parser.add_argument('--fixed-date', action='store_true',
                        help='Use fixed date 2025-03-31 for initial release')
    parser.add_argument('--visible', action='store_true',
                        help='Run browser in visible mode (not headless)')

    args = parser.parse_args()

    # Override headless setting if visible flag is set
    if args.visible:
        import config
        config.HEADLESS = True

    print("\n" + "=" * 60)
    print("KOFIA Mutual Fund Data Scraper")
    print("=" * 60)
    print(f"Browser: {BROWSER}")
    print(f"Headless: {not args.visible}")
    print(f"Download Directory: {DOWNLOAD_DIR}")
    print(f"Log Directory: {LOG_DIR}")
    print(f"Using fixed date: {args.fixed_date}")
    print("=" * 60 + "\n")

    scraper = KOFIAScraper(use_fixed_date=args.fixed_date)
    scraper.run()
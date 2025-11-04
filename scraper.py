"""
=============================================================================
CONFIGURATION - Scraper Settings
=============================================================================
"""
# Set to True to run browser in background (headless mode)
# Set to False to see the browser window (useful for debugging)
HEADLESS_MODE = True  # Change this to True/False as needed

# Number of parallel browser tabs to run (1-3 recommended)
# More tabs = faster downloads, but uses more system resources
NUM_PARALLEL_TABS = 1  # Change to 1 for single-threaded, 3 for 3x speed

"""
=============================================================================
"""

import os
import time
import logging
import datetime
import pandas as pd
import zipfile
import re
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import undetected_chromedriver as uc
from selenium_stealth import stealth

from config import (
    BASE_URL, BROWSER, USE_STEALTH, IMPLICIT_WAIT, EXPLICIT_WAIT_TIMEOUT, DOWNLOAD_WAIT_TIMEOUT,
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
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException,
    WebDriverException
)

# Set up logging - both file and console
current_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = os.path.join(LOG_DIR, f"kofia_scraper_{current_timestamp}.log")

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# File handler
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S'))

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

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
        Initializes and returns the Selenium WebDriver with improved version handling.
        """
        try:
            if BROWSER == "chrome":
                if USE_STEALTH:
                    mode_str = "HEADLESS" if HEADLESS_MODE else "VISIBLE"
                    logger.info(f"Initializing Chrome in STEALTH mode ({mode_str})")
                    options = get_stealth_chrome_options(headless=HEADLESS_MODE)
                    try:
                        driver = uc.Chrome(options=options, use_subprocess=True, version_main=None)
                        logger.info("✓ Driver initialized with auto-detected version.")
                    except Exception as e:
                        logger.error(f"Stealth mode failed completely: {e}. Falling back to regular Chrome...")
                        options = get_chrome_options(headless=HEADLESS_MODE)
                        driver = webdriver.Chrome(options=options)

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
                    
                    if hasattr(driver, 'execute_script'):
                        try:
                            # Enhanced anti-detection script to hide automation traces
                            driver.execute_script("""
                                // Delete webdriver property from navigator
                                delete navigator.__proto__.webdriver;
                                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

                                // Mock plugins to look like a real browser
                                Object.defineProperty(navigator, 'plugins', {
                                    get: () => [1, 2, 3, 4, 5]
                                });

                                // Mock languages
                                Object.defineProperty(navigator, 'languages', {
                                    get: () => ['ko-KR', 'ko', 'en-US', 'en']
                                });

                                // Mock chrome object
                                window.chrome = {
                                    runtime: {},
                                    loadTimes: function() {},
                                    csi: function() {},
                                    app: {}
                                };

                                // Mock permissions
                                const originalQuery = window.navigator.permissions.query;
                                window.navigator.permissions.query = (parameters) => (
                                    parameters.name === 'notifications' ?
                                        Promise.resolve({ state: Notification.permission }) :
                                        originalQuery(parameters)
                                );

                                // Hide automation in window.navigator.webdriver
                                Object.defineProperty(window, 'navigator', {
                                    value: new Proxy(navigator, {
                                        get: (target, prop) => {
                                            if (prop === 'webdriver') return undefined;
                                            return target[prop];
                                        }
                                    })
                                });
                            """)
                            logger.info("✓ Enhanced anti-detection JavaScript applied")
                        except Exception as e:
                            logger.warning(f"Could not apply JS anti-detection: {e}")
                else:
                    mode_str = "HEADLESS" if HEADLESS_MODE else "VISIBLE"
                    logger.info(f"Initializing Chrome in standard mode ({mode_str})")
                    options = get_chrome_options(headless=HEADLESS_MODE)
                    driver = webdriver.Chrome(options=options)
            elif BROWSER == "firefox":
                options = get_firefox_options()
                driver = webdriver.Firefox(options=options)
            else:
                raise ValueError(f"Unsupported browser: {BROWSER}")

            driver.implicitly_wait(IMPLICIT_WAIT)
            driver.set_page_load_timeout(60)
            logger.info(f"WebDriver for {BROWSER} initialized successfully (Stealth: {USE_STEALTH}).")
            return driver
        except WebDriverException as e:
            logger.error(f"Failed to initialize WebDriver for {BROWSER}: {e}")
            raise

    def find_element_safely(self, locators, timeout=EXPLICIT_WAIT_TIMEOUT, screenshot_name=None, parent_element=None):
        """
        Attempts to find an element using multiple locator strategies with retry logic.
        Can search within a parent element if provided.
        """
        search_context = parent_element if parent_element else self.driver
        for attempt in range(MAX_RETRIES):
            for by_strategy, locator_value in locators:
                try:
                    logger.debug(f"Attempt {attempt + 1}: Trying to find element by {by_strategy} with value '{locator_value}'")
                    element = WebDriverWait(search_context, timeout).until(
                        EC.presence_of_element_located((by_strategy, locator_value))
                    )
                    logger.info(f"Element found: {locator_value}")
                    return element
                except (NoSuchElementException, TimeoutException, StaleElementReferenceException) as e:
                    logger.warning(f"Attempt {attempt + 1}: Locator '{locator_value}' failed: {e.__class__.__name__}. Trying next locator.")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"All locators failed for current attempt. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY + random.uniform(0, 2))
            else:
                error_msg = f"Failed to find element after {MAX_RETRIES} attempts using any provided locators."
                logger.error(error_msg)
                if screenshot_name:
                    self.capture_screenshot(screenshot_name)
                raise NoSuchElementException(error_msg)
        return None

    def capture_screenshot(self, name="error_screenshot"):
        """Captures a screenshot of the current page."""
        screenshot_path = os.path.join(LOG_DIR, f"{name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.png")
        try:
            self.driver.save_screenshot(screenshot_path)
            logger.error(f"Screenshot captured: {screenshot_path}")
        except Exception as e:
            logger.error(f"Failed to capture screenshot: {e}")

    def ensure_main_frame(self):
        """Ensures we're in the 'main' frame."""
        try:
            self.driver.find_element(By.XPATH, "//img[@alt='펀드']")
            logger.debug("Already in main frame")
        except:
            try:
                self.driver.switch_to.default_content()
                self.driver.switch_to.frame("main")
                logger.info("Re-switched to main frame")
            except Exception as e:
                logger.error(f"Failed to switch to main frame: {e}")

    def wait_for_download_completion(self, initial_files, timeout=DOWNLOAD_WAIT_TIMEOUT):
        """Waits for a new file to appear in the download directory."""
        start_time = time.time()
        logger.info("Waiting for file download to complete...")
        while time.time() - start_time < timeout:
            current_files = set(os.listdir(DOWNLOAD_DIR))
            new_files = current_files - initial_files
            for fname in new_files:
                if not fname.endswith(('.tmp', '.crdownload', '.part')):
                    new_file_path = os.path.join(DOWNLOAD_DIR, fname)
                    current_size = -1
                    for _ in range(5):
                        time.sleep(0.2)
                        if not os.path.exists(new_file_path): break
                        new_size = os.path.getsize(new_file_path)
                        if current_size == new_size and new_size > 0:
                            logger.info(f"Download complete: {new_file_path}")
                            return new_file_path
                        current_size = new_size
                    if new_file_path and os.path.exists(new_file_path):
                        logger.info(f"Download complete (size check passed): {new_file_path}")
                        return new_file_path
            time.sleep(1)
        self.capture_screenshot("download_timeout")
        raise TimeoutException(f"Download did not complete within {timeout} seconds.")

    def _select_from_custom_dropdown(self, button_locators, option_text, screenshot_name_prefix, max_retries=3):
        """
        Finds a custom dropdown, clicks it to open, and clicks the desired option.
        Enhanced with retries and verification to handle DOM changes between datasets.
        """
        for retry in range(max_retries):
            dropdown_start = time.time()
            actions = ActionChains(self.driver)
            listbox_element = None

            try:
                # 1. Find the visible dropdown button
                logger.debug(f"[Dropdown Attempt {retry+1}/{max_retries}] Locating dropdown button for '{option_text}'...")

                # Find all matching buttons and pick the visible one
                dropdown_button = None
                for by_strategy, locator_value in button_locators:
                    try:
                        buttons = self.driver.find_elements(by_strategy, locator_value)
                        for btn in buttons:
                            if btn.is_displayed():
                                dropdown_button = btn
                                logger.debug(f"[Dropdown] Found visible dropdown button")
                                break
                        if dropdown_button:
                            break
                    except:
                        continue

                if not dropdown_button:
                    dropdown_button = self.find_element_safely(button_locators, screenshot_name=f"{screenshot_name_prefix}_button_not_found")

                # Check current value of dropdown - the text is in a nested div with id starting with 'cb-text-'
                try:
                    current_value = None
                    # Try to find the text div inside the dropdown button
                    try:
                        text_div = dropdown_button.find_element(By.XPATH, ".//div[starts-with(@id, 'cb-text-')]")
                        current_value = text_div.text.strip()
                    except:
                        # Fallback to button text
                        current_value = dropdown_button.text.strip()

                    logger.debug(f"[Dropdown] Current value: '{current_value}', target: '{option_text}'")
                    if current_value == option_text:
                        logger.info(f"[Dropdown] ✓ Dropdown already set to '{option_text}', skipping")
                        return True
                except Exception as e:
                    logger.debug(f"[Dropdown] Could not check current value: {e}")
                    pass

                # Log button state before click
                aria_expanded_before = dropdown_button.get_attribute('aria-expanded')
                logger.debug(f"[Dropdown] Button state BEFORE click: aria-expanded={aria_expanded_before}")

                # If dropdown is already expanded, close it first
                if aria_expanded_before == 'true':
                    logger.debug(f"[Dropdown] Dropdown already expanded, closing it first...")
                    self.driver.execute_script("arguments[0].click();", dropdown_button)
                    time.sleep(1)

                # Scroll into view
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", dropdown_button)
                time.sleep(1)

                # Try multiple click methods to open dropdown
                clicked = False
                for click_method in ['actionchains', 'javascript', 'direct']:
                    try:
                        if click_method == 'actionchains':
                            actions.move_to_element(dropdown_button).pause(0.3).click().perform()
                        elif click_method == 'javascript':
                            self.driver.execute_script("arguments[0].click();", dropdown_button)
                        else:
                            dropdown_button.click()

                        time.sleep(1.5)
                        aria_expanded_after = dropdown_button.get_attribute('aria-expanded')

                        if aria_expanded_after == 'true':
                            logger.debug(f"[Dropdown] Opened using {click_method}")
                            clicked = True
                            break
                    except Exception as e:
                        logger.debug(f"[Dropdown] {click_method} failed: {e}")

                if not clicked:
                    raise Exception("Could not open dropdown with any click method")

                # 2. Wait for the dropdown list to appear
                logger.debug(f"[Dropdown] Waiting for dropdown list to appear...")
                time.sleep(1)

                # Try to find listbox in main frame
                listbox_in_main_frame = False
                try:
                    listbox_element = self.wait.until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'cl-listbox')] | //div[@role='listbox']"))
                    )
                    logger.debug(f"[Dropdown] Listbox found in main frame")
                    listbox_in_main_frame = True
                except:
                    # Listbox is in parent frame (outside iframe)
                    logger.debug(f"[Dropdown] Listbox not in iframe, switching to default content...")
                    self.driver.switch_to.default_content()
                    time.sleep(0.5)
                    dropdown_list_locators = [
                        (By.XPATH, "//div[contains(@class, 'cl-listbox')]"),
                        (By.XPATH, "//div[@role='listbox']"),
                    ]
                    listbox_element = self.find_element_safely(dropdown_list_locators, timeout=5, screenshot_name=f"{screenshot_name_prefix}_list_not_found")
                    listbox_in_main_frame = False

                logger.debug(f"[Dropdown] Listbox element located")
                time.sleep(1)

                # IMPORTANT: Scroll listbox back to top to ensure we can find all options
                try:
                    self.driver.execute_script("arguments[0].scrollTop = 0;", listbox_element)
                    time.sleep(0.2)
                    logger.debug(f"[Dropdown] Reset listbox scroll to top")
                except:
                    pass

                # Debug: Log all available options to understand dropdown content
                try:
                    all_options_elements = listbox_element.find_elements(By.XPATH, ".//div[contains(@class, 'cl-text')]")
                    all_option_texts = []
                    for opt in all_options_elements[:20]:  # First 20 options
                        try:
                            text = opt.text
                            if text:
                                all_option_texts.append(text)
                        except:
                            pass
                    logger.info(f"[Dropdown] Available options (first 20): {all_option_texts}")
                    logger.info(f"[Dropdown] Looking for: '{option_text}'")
                except Exception as e:
                    logger.debug(f"[Dropdown] Could not log available options: {e}")

                # Debug: Log listbox HTML structure when looking for specific options
                if option_text in ['혼합자산', '단기금융']:
                    try:
                        listbox_html = listbox_element.get_attribute('outerHTML')[:500]
                        logger.debug(f"[Dropdown] Listbox HTML (first 500 chars): {listbox_html}")
                    except:
                        pass

                # 3. Find and click the specific option (with scrolling support for long lists)
                logger.debug(f"[Dropdown] Searching for option '{option_text}' in listbox...")

                option_xpaths = [
                    f".//li[@role='option']//div[contains(@class, 'cl-text') and normalize-space()='{option_text}']",
                    f".//div[contains(@class, 'cl-text') and normalize-space()='{option_text}']",
                    f".//*[normalize-space()='{option_text}']",
                    f".//div[contains(text(), '{option_text}')]",  # More flexible - contains instead of exact match
                    f".//*[contains(text(), '{option_text}')]"     # Very flexible - any element containing text
                ]

                option_element = None

                # First attempt: Try to find without scrolling (quick check with find_elements)
                # Temporarily disable implicit wait for speed
                original_implicit_wait = self.driver.timeouts.implicit_wait
                self.driver.implicitly_wait(0)

                try:
                    for xpath in option_xpaths:
                        try:
                            elements = listbox_element.find_elements(By.XPATH, xpath)
                            if elements:
                                option_element = elements[0]
                                logger.debug(f"[Dropdown] Found option using xpath: {xpath}")
                                break
                        except:
                            continue
                finally:
                    # Restore original implicit wait
                    self.driver.implicitly_wait(original_implicit_wait)

                # If not found, the option might be below the visible area - try scrolling
                if not option_element:
                    logger.debug(f"[Dropdown] Option not immediately visible, starting manual scroll...")

                    try:
                        # Get visible options for debugging
                        all_visible_options = listbox_element.find_elements(By.XPATH, ".//div[contains(@class, 'cl-text')]")
                        visible_texts = []
                        for opt in all_visible_options[:10]:
                            try:
                                text = opt.text
                                visible_texts.append(text)
                            except:
                                pass
                        logger.debug(f"[Dropdown] Initially visible options: {visible_texts}")

                        # Find the scrollable container (might be listbox or a parent)
                        scrollable_containers = [listbox_element]

                        try:
                            parent = listbox_element.find_element(By.XPATH, "./parent::*")
                            scrollable_containers.append(parent)
                        except:
                            pass

                        try:
                            ancestor = listbox_element.find_element(By.XPATH, "./ancestor::div[contains(@class, 'cl-listbox')][1]")
                            scrollable_containers.append(ancestor)
                        except:
                            pass

                        logger.debug(f"[Dropdown] Found {len(scrollable_containers)} scrollable containers to try")

                        max_scroll_attempts = 50  # Increased from 30 to 50 for more thorough search
                        scroll_amount = 100  # Larger scroll amount to trigger lazy loading

                        # Disable implicit wait during scrolling for speed
                        self.driver.implicitly_wait(0)

                        try:
                            # Check if container has scrollable content
                            has_scrollable_container = False
                            for container in scrollable_containers:
                                try:
                                    max_scroll_height = self.driver.execute_script("return arguments[0].scrollHeight - arguments[0].clientHeight;", container)
                                    if max_scroll_height > 0:
                                        has_scrollable_container = True
                                        logger.debug(f"[Dropdown] Container is scrollable (max scroll: {max_scroll_height}px)")
                                        break
                                except:
                                    continue

                            if not has_scrollable_container:
                                logger.debug(f"[Dropdown] No scrollable container detected (scrollHeight = clientHeight), will use mouse wheel for lazy-loaded DOM")

                            # Search from top to bottom using mouse wheel (works for lazy-loaded DOM)
                            logger.debug(f"[Dropdown] Searching through dropdown using mouse wheel scrolling...")
                            for scroll_attempt in range(max_scroll_attempts):
                                # Try to find our option after each scroll (use find_elements for speed)
                                for xpath in option_xpaths:
                                    try:
                                        elements = listbox_element.find_elements(By.XPATH, xpath)
                                        if elements:
                                            option_element = elements[0]
                                            logger.debug(f"[Dropdown] ✓ Found option after {scroll_attempt} scroll attempts: {xpath}")
                                            break
                                    except:
                                        continue

                                if option_element:
                                    break

                                # Log current visible options every 10 scrolls
                                if scroll_attempt % 10 == 0 and scroll_attempt > 0:
                                    visible_now = []
                                    current_visible = listbox_element.find_elements(By.XPATH, ".//div[contains(@class, 'cl-text')]")
                                    for opt in current_visible[:5]:
                                        try:
                                            text = opt.text
                                            visible_now.append(text)
                                        except:
                                            pass
                                    logger.debug(f"[Dropdown] After {scroll_attempt} scrolls, visible options: {visible_now}")

                                # Use mouse wheel to scroll and trigger lazy loading
                                try:
                                    actions = ActionChains(self.driver)
                                    actions.move_to_element(listbox_element).perform()
                                    time.sleep(0.1)

                                    # Scroll down using mouse wheel
                                    self.driver.execute_script("""
                                        var element = arguments[0];
                                        var event = new WheelEvent('wheel', {
                                            deltaY: 150,
                                            bubbles: true,
                                            cancelable: true
                                        });
                                        element.dispatchEvent(event);
                                    """, listbox_element)
                                    time.sleep(0.15)
                                    logger.debug(f"[Dropdown] Mouse wheel scroll attempt {scroll_attempt+1}")
                                except Exception as e:
                                    logger.debug(f"[Dropdown] Mouse wheel scroll failed: {e}")
                                    break
                        finally:
                            # Restore implicit wait after scrolling
                            self.driver.implicitly_wait(original_implicit_wait)

                    except Exception as e:
                        logger.debug(f"[Dropdown] Mouse scrolling attempt failed: {e}")

                if not option_element:
                    # Option not found - close dropdown and let retry mechanism handle it
                    logger.warning(f"[Dropdown] Could not find '{option_text}' - will retry dropdown")
                    try:
                        # Click dropdown button to close it
                        if not listbox_in_main_frame:
                            self.driver.switch_to.frame(self.driver.find_element(By.NAME, "main"))
                        self.driver.execute_script("arguments[0].click();", dropdown_button)
                        time.sleep(0.5)
                    except:
                        pass
                    raise Exception(f"Could not find option '{option_text}' in listbox even after scrolling")

                logger.debug(f"[Dropdown] Option '{option_text}' found, clicking...")

                # Click the option - try JavaScript first
                try:
                    self.driver.execute_script("arguments[0].click();", option_element)
                    logger.debug(f"[Dropdown] Option clicked using JavaScript")
                except Exception as e:
                    logger.debug(f"[Dropdown] JavaScript click failed ({e}), trying ActionChains...")
                    actions.move_to_element(option_element).click().perform()

                dropdown_duration = time.time() - dropdown_start
                logger.info(f"[Dropdown] ✓ Selected '{option_text}' successfully ({dropdown_duration:.2f}s)")
                time.sleep(1.5)

                # Switch back to main frame if we switched out
                if not listbox_in_main_frame:
                    logger.debug(f"[Dropdown] Switching back to main frame...")
                    self.driver.switch_to.frame("main")
                    time.sleep(0.5)

                # Verify selection was successful
                try:
                    time.sleep(1)
                    current_value = dropdown_button.text.strip()
                    if current_value == option_text:
                        logger.info(f"[Dropdown] ✓ Verified selection: '{option_text}'")
                        return True
                    else:
                        logger.warning(f"[Dropdown] Selection may not have stuck. Current: '{current_value}', Expected: '{option_text}'")
                        if retry < max_retries - 1:
                            continue
                except Exception as e:
                    logger.debug(f"Could not verify selection: {e}")

                return True

            except Exception as e:
                logger.warning(f"[Dropdown] Attempt {retry+1}/{max_retries} to select '{option_text}' failed: {e}")
                self.capture_screenshot(f"{screenshot_name_prefix}_select_error_attempt_{retry+1}")

                # Switch back to main frame before retry
                try:
                    self.driver.switch_to.default_content()
                    self.driver.switch_to.frame("main")
                except:
                    pass

                if retry < max_retries - 1:
                    logger.info(f"[Dropdown] Retrying in 2 seconds...")
                    time.sleep(2)
                else:
                    logger.error(f"[Dropdown] Failed to select '{option_text}' after {max_retries} attempts")
                    raise

        return False

    def _select_radio_button_improved(self, radio_label, section_name, dataset_name):
        """
        Improved method to select a radio button by finding its section first.
        This version is more aggressive with debugging to capture the page source.
        """
        logger.info(f"Step: Selecting {section_name}: {radio_label}...")
        success = False
        radio_element = None

        # --- DEBUGGING STEP: Save page source immediately ---
        debug_file_name = f"debug_page_source_{dataset_name}_{section_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.html"
        debug_file_path = os.path.join(LOG_DIR, debug_file_name)
        try:
            with open(debug_file_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            logger.error(f"DEBUG: Page source saved to {debug_file_path}. Please provide this file for analysis.")
            print(f"\n!!! FOR DEBUGGING: A page source file has been saved to:\n{debug_file_path}\n!!! Please provide this file to continue.\n")
        except Exception as save_error:
            logger.warning(f"Could not save page source for debugging: {save_error}")

        try:
            # --- STRATEGY 1: Find the section first, then the radio button within it ---
            logger.debug(f"[Radio] Strategy 1: Attempting to find section '{section_name}' first.")
            section_locators = [
                # Look for a div that contains the section text and the radio button
                (By.XPATH, f"//div[contains(@class, 'cl-form-item') and normalize-space(.)='{section_name}']"),
                (By.XPATH, f"//div[contains(@class, 'cl-form-item') and contains(., '{section_name}')]"),
                # Look for a label and then find the following radio group
                (By.XPATH, f"//label[contains(text(), '{section_name}')]/following-sibling::div[contains(@class, 'cl-radiogroup')]"),
                (By.XPATH, f"//*[normalize-space(text())='{section_name}']/following-sibling::div[contains(@class, 'cl-radiogroup')]"),
                # More generic searches for the section
                (By.XPATH, f"//*[contains(text(), '{section_name}')]//ancestor::div[contains(@class, 'cl-form-item')]"),
                (By.XPATH, f"//*[contains(text(), '{section_name}')]//ancestor::div[contains(@class, 'cl-radiogroup')]"),
            ]
            
            section_element = None
            try:
                section_element = self.find_element_safely(section_locators, timeout=10, screenshot_name=f"{dataset_name}_{section_name}_section_not_found")
            except NoSuchElementException:
                logger.warning(f"[Radio] Strategy 1 failed: Could not find the section '{section_name}'.")

            if section_element:
                logger.debug(f"[Radio] Section found. Now searching for radio button '{radio_label}' within it.")
                radio_locators = [
                    (By.XPATH, f".//span[@role='radio' and @aria-label='{radio_label}']"),
                    (By.XPATH, f".//label[text()='{radio_label}']/preceding-sibling::div//span[@role='radio']"),
                    (By.XPATH, f".//label[normalize-space(text())='{radio_label}']/preceding-sibling::div//span[@role='radio']"),
                ]
                try:
                    radio_element = self.find_element_safely(radio_locators, timeout=5, parent_element=section_element, screenshot_name=f"{dataset_name}_{section_name}_radio_not_found")
                except NoSuchElementException:
                    logger.warning(f"[Radio] Strategy 1 failed: Could not find radio button '{radio_label}' within the located section.")

            # --- STRATEGY 2: Fallback - Find all matching radio buttons and verify context ---
            if not radio_element:
                logger.warning(f"[Radio] Strategy 1 failed. Trying Strategy 2: Global search with context verification.")
                all_radio_buttons = self.driver.find_elements(By.XPATH, f"//span[@role='radio' and @aria-label='{radio_label}']")
                logger.info(f"[Radio] Strategy 2: Found {len(all_radio_buttons)} radio buttons with label '{radio_label}'. Verifying context.")

                for rb in all_radio_buttons:
                    # Check if the section name exists in the parent hierarchy
                    try:
                        parent_section = rb.find_element(By.XPATH, f".//ancestor::*[contains(., '{section_name}')][1]")
                        # A simple check to see if the found ancestor is a reasonable container
                        if "form" in parent_section.get_attribute("class") or "item" in parent_section.get_attribute("class"):
                            radio_element = rb
                            logger.info(f"[Radio] Strategy 2: Found correct radio button by verifying its parent context.")
                            break
                    except NoSuchElementException:
                        continue # This radio button is not in the right section, try the next one
            
            if not radio_element:
                # --- DESPERATE FALLBACK ---
                logger.warning(f"[Radio] All strategies failed. Trying a desperate fallback to find any radio button with label '{radio_label}'.")
                try:
                    # This is very brittle, but might work if there's only one group of these radio buttons
                    radio_element = self.driver.find_element(By.XPATH, f"//span[@role='radio' and @aria-label='{radio_label}']")
                    logger.info(f"[Radio] Desperate fallback found a radio button.")
                except NoSuchElementException:
                    error_msg = f"Could not find radio button '{radio_label}' for section '{section_name}' using any strategy or fallback."
                    logger.error(error_msg)
                    self.capture_screenshot(f"{dataset_name}_{section_name}_select_error")
                    raise Exception(error_msg)

            # --- Click and Verify ---
            aria_checked_before = radio_element.get_attribute('aria-checked')
            logger.debug(f"[Radio] {section_name} '{radio_label}' state BEFORE click: aria-checked={aria_checked_before}")

            if aria_checked_before != 'true':
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", radio_element)
                time.sleep(0.5)
                
                # Try clicking the label first, as it's often a larger target
                try:
                    # Find the label associated with this specific radio button
                    label_element = radio_element.find_element(By.XPATH, "./following-sibling::label | ./ancestor::div//label[normalize-space(.)='{radio_label}']")
                    self.driver.execute_script("arguments[0].click();", label_element)
                    logger.debug(f"[Radio] Clicked label for {section_name} '{radio_label}'")
                except NoSuchElementException:
                    # If label is not found or not clickable, click the radio span itself
                    self.driver.execute_script("arguments[0].click();", radio_element)
                    logger.debug(f"[Radio] Clicked radio span for {section_name} '{radio_label}'")
                
                time.sleep(0.5)

                aria_checked_after = radio_element.get_attribute('aria-checked')
                logger.debug(f"[Radio] {section_name} '{radio_label}' state AFTER click: aria-checked={aria_checked_after}")
                
                if aria_checked_after == 'true':
                    success = True
                    logger.info(f"✓ Selected {section_name}: {radio_label}")
                else:
                    logger.warning(f"Failed to select {section_name}: {radio_label}. State did not change.")
            else:
                success = True
                logger.info(f"✓ {section_name}: {radio_label} was already selected.")
        
        except Exception as e:
            logger.error(f"Error selecting {section_name}: {radio_label} - {e}")
            self.capture_screenshot(f"{dataset_name}_{section_name}_select_error")
        
        return success

    def navigate_to_fund_composition_query(self):
        """
        Navigates to the fund composition query page with detailed step-by-step logging.
        """
        nav_step = 1
        logger.info(f"[Navigation Step {nav_step}] Loading base URL: {BASE_URL}")
        self.driver.get(BASE_URL)
        logger.info(f"[Navigation Step {nav_step}] ✓ Page loaded")
        nav_step += 1

        try:
            logger.info(f"[Navigation Step {nav_step}] Switching to 'main' frame...")
            self.driver.switch_to.frame("main")
            logger.info(f"[Navigation Step {nav_step}] ✓ Successfully switched to main frame")
            wait_time = random.uniform(2, 4)
            logger.debug(f"[Navigation] Waiting {wait_time:.2f}s for frame to stabilize")
            time.sleep(wait_time)
            nav_step += 1
        except Exception as e:
            logger.error(f"[Navigation Step {nav_step}] ✗ Failed to switch to main frame: {e}")
            self.capture_screenshot("frame_switch_failed")
            raise

        logger.info(f"[Navigation Step {nav_step}] Locating Fund (펀드) tab...")
        fund_tab = self.find_element_safely(NAV_FUND_TAB, screenshot_name="fund_tab_not_found")
        fund_tab.click()
        logger.info(f"[Navigation Step {nav_step}] ✓ Clicked on Fund (펀드) tab")
        wait_time = random.uniform(4, 6)
        logger.debug(f"[Navigation] Waiting {wait_time:.2f}s for sidebar to load")
        time.sleep(wait_time)
        nav_step += 1

        try:
            actions = ActionChains(self.driver)

            # Step: Click "펀드산업" (Fund Industry)
            logger.info(f"[Navigation Step {nav_step}] Locating '펀드산업' (Fund Industry) menu item...")
            fund_industry_locators = [(By.XPATH, "//div[@data-itemid='MSIS40100000000000']")]
            fund_industry = self.find_element_safely(fund_industry_locators, timeout=15, screenshot_name="fund_industry_not_found")
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", fund_industry)
            time.sleep(1)
            actions.move_to_element(fund_industry).click().perform()
            logger.info(f"[Navigation Step {nav_step}] ✓ Clicked '펀드산업'")
            wait_time = random.uniform(5, 7)  # Increased from 3-4 to 5-7 for parallel tabs
            logger.debug(f"[Navigation] Waiting {wait_time:.2f}s for submenu to expand")
            time.sleep(wait_time)
            nav_step += 1

            # Step: Expand "운용통계" (Management Statistics) submenu by clicking the expand icon
            logger.info(f"[Navigation Step {nav_step}] Locating '운용통계' (Management Statistics) expand icon...")

            # First, find the '운용통계' menu item to locate its expand icon
            mgmt_stats_locators = [(By.XPATH, "//div[contains(@class, 'cl-tree-item')][@title='운용통계']")]
            mgmt_stats = self.find_element_safely(mgmt_stats_locators, timeout=20, screenshot_name="mgmt_stats_not_found")  # Increased from 10 to 20

            # Wait for element to be clickable (not just visible)
            try:
                from selenium.webdriver.support import expected_conditions as EC
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'cl-tree-item')][@title='운용통계']")))
                logger.debug(f"[Navigation] '운용통계' element is now clickable")
            except:
                logger.debug(f"[Navigation] Clickable wait timed out, proceeding anyway")

            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", mgmt_stats)
            time.sleep(1)

            # Find the expand icon within the '운용통계' item (the cl-tree-treeicon element)
            logger.debug(f"[Navigation Step {nav_step}] Finding expand icon for '운용통계'...")
            expand_icon_locators = [
                (By.XPATH, "//div[contains(@class, 'cl-tree-item')][@title='운용통계']//div[contains(@class, 'cl-tree-treeicon')]"),
                (By.XPATH, "//div[@title='운용통계']//preceding-sibling::div[contains(@class, 'cl-tree-treeicon')]"),
                (By.XPATH, "//div[@title='운용통계']/../..//div[contains(@class, 'cl-tree-treeicon')]"),
            ]
            expand_icon = self.find_element_safely(expand_icon_locators, timeout=10, screenshot_name="mgmt_stats_expand_icon_not_found")

            # Click the expand icon to expand submenu (with retry if it doesn't expand)
            max_expand_attempts = 3
            submenu_expanded = False

            for expand_attempt in range(max_expand_attempts):
                logger.info(f"[Navigation] Clicking '운용통계' expand icon (attempt {expand_attempt + 1}/{max_expand_attempts})...")

                # Click the expand icon
                try:
                    actions.move_to_element(expand_icon).click().perform()
                    time.sleep(2)
                except:
                    # If ActionChains fails, try JavaScript click
                    self.driver.execute_script("arguments[0].click();", expand_icon)
                    time.sleep(2)

                # Check if submenu appeared by looking for '투자자산별비중'
                try:
                    submenu_check = self.driver.find_elements(By.XPATH, "//div[@title='투자자산별비중']")
                    if submenu_check and any(el.is_displayed() for el in submenu_check):
                        submenu_expanded = True
                        logger.info(f"[Navigation Step {nav_step}] ✓ Submenu expanded successfully")
                        break
                    else:
                        logger.warning(f"[Navigation] Submenu not visible after click attempt {expand_attempt + 1}, retrying...")
                        time.sleep(2)
                except:
                    logger.warning(f"[Navigation] Could not verify submenu after click attempt {expand_attempt + 1}, retrying...")
                    time.sleep(2)

            if not submenu_expanded:
                logger.warning("[Navigation] Submenu may not have expanded fully, but proceeding anyway...")

            self.capture_screenshot("after_mgmt_stats_submenu_wait")
            nav_step += 1

            # Step 3: Click "투자자산별비중" (Investment Asset Weight/Composition)
            logger.info("Step 3: Clicking '투자자산별비중' menu item...")
            asset_weight_locators = [
                # More specific locators looking for the item inside the expanded menu
                (By.XPATH, "//div[contains(@class, 'cl-tree-item') and @title='운용통계']//following-sibling::div//div[@title='투자자산별비중']"),
                (By.XPATH, "//div[contains(@class, 'cl-tree-item') and @title='운용통계']/..//div[@title='투자자산별비중']"),
                # Fallback to broader searches
                (By.XPATH, "//div[@class='sub-items']//div[contains(@class, 'cl-tree-item')][@title='투자자산별비중']"),
                (By.XPATH, "//div[@class='sub-items']//div[contains(@class, 'cl-text') and text()='투자자산별비중']"),
                (By.XPATH, "//div[contains(@class, 'cl-tree-item')][@title='투자자산별비중']"),
                (By.XPATH, "//*[text()='투자자산별비중']"),
            ]
            asset_weight = self.find_element_safely(asset_weight_locators, timeout=15, screenshot_name="asset_weight_not_found")
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", asset_weight)
            time.sleep(1)
            actions.move_to_element(asset_weight).click().perform()
            logger.info("✓ Clicked '투자자산별비중' using ActionChains")
            time.sleep(random.uniform(4, 6))

            # Step 4: Click on "추이" (Trend/Time Series) tab
            logger.info("Step 4: Clicking '추이' (Trend) tab...")
            trend_tab_locators = [
                (By.XPATH, "//div[contains(@class, 'cl-tabfolder-item')]//div[@class='cl-text' and text()='추이']"),
                (By.XPATH, "//div[@role='tab' and text()='추이']"),
                (By.XPATH, "//div[contains(@class, 'cl-text') and text()='추이']"),
                (By.XPATH, "//*[text()='추이']"),
            ]
            trend_tab = self.find_element_safely(trend_tab_locators, timeout=10, screenshot_name="trend_tab_not_found")
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", trend_tab)
            time.sleep(1)
            actions.move_to_element(trend_tab).click().perform()
            logger.info("✓ Step 4 Complete: Clicked '추이' tab - query form page ready")
            time.sleep(random.uniform(3, 5))
        except NoSuchElementException as e:
            logger.error(f"Failed to navigate through sidebar menu: {e}")
            self.capture_screenshot("sidebar_navigation_failed")
            try:
                page_source_path = os.path.join(LOG_DIR, f"page_source_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.html")
                with open(page_source_path, 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                logger.info(f"Page source saved to: {page_source_path}")
            except Exception as save_error:
                logger.warning(f"Could not save page source: {save_error}")
            raise
        logger.info("Fund composition query form page loaded")

    def download_dataset(self, config):
        """
        Configures the form, searches, and downloads a single dataset.
        Uses the new _select_from_custom_dropdown helper for dropdowns.
        """
        dataset_name = config['name']
        output_name = config['output_name']
        fund_type_korean = config['fund_type_korean']
        region_korean = config['region_korean']
        time_window_years = config['time_window_years']

        logger.info(f"--- Starting download for dataset: {dataset_name} ---")
        success = False
        actions = ActionChains(self.driver)

        for attempt in range(MAX_RETRIES):
            try:
                # Ensure we're in main frame and page is ready
                self.ensure_main_frame()
                logger.info(f"Attempt {attempt + 1}: Configuring form for {dataset_name}")

                # Check for and close any lingering popups/alerts from previous dataset
                try:
                    alert = self.driver.switch_to.alert
                    alert.accept()
                    logger.info("Closed lingering alert popup before starting")
                    time.sleep(1)
                    self.driver.switch_to.frame("main")
                except:
                    pass

                # Wait for page to be stable
                time.sleep(random.uniform(3, 5))

                # Step 0: Set the period dropdown (조회기간)
                if time_window_years >= 1:
                    period_text = f"{time_window_years}년"
                else:
                    period_months = time_window_years * 12
                    period_text = f"{period_months}개월"
                logger.info(f"Step 0: Setting query period to {period_text}...")
                period_button_locators = [
                    (By.XPATH, "//div[@title='조회기간(월단위)']//div[@role='combobox']"),
                    (By.XPATH, "//div[@title='조회기간(월단위)']"),
                ]
                self._select_from_custom_dropdown(period_button_locators, period_text, f"{dataset_name}_period")
                self.capture_screenshot(f"{dataset_name}_after_period_select")

                logger.info("Waiting for page to stabilize after period selection...")
                time.sleep(3)

                # Step 1: Select Fund Type (펀드유형)
                logger.info(f"Step 1: Selecting Fund Type (펀드유형): {fund_type_korean}...")

                # Define expected fund types for validation
                expected_fund_types = ['주식형', '혼합주식형', '혼합채권형', '채권형', '단기금융', '전체']

                # Find all combobox dropdowns on the page
                all_dropdowns = self.driver.find_elements(By.XPATH, "//div[@role='combobox']")
                logger.debug(f"Found {len(all_dropdowns)} total combobox dropdowns on page")

                # Try to find the correct 펀드유형 dropdown by checking its options
                correct_dropdown = None
                for idx, dropdown in enumerate(all_dropdowns):
                    try:
                        # Get the title or nearby label
                        dropdown_title = dropdown.get_attribute('title') or ''
                        dropdown_text = dropdown.text or ''

                        # Check parent element for title
                        parent_title = ''
                        try:
                            parent = dropdown.find_element(By.XPATH, "./parent::*")
                            parent_title = parent.get_attribute('title') or ''
                        except:
                            pass

                        logger.debug(f"Dropdown {idx}: title='{dropdown_title}', parent_title='{parent_title}', text='{dropdown_text[:30]}'")

                        # Click to open and check options
                        if '펀드유형' in dropdown_title or '펀드유형' in parent_title:
                            logger.debug(f"Found potential 펀드유형 dropdown at index {idx}, opening to verify...")

                            # Open dropdown
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", dropdown)
                            time.sleep(0.5)
                            self.driver.execute_script("arguments[0].click();", dropdown)
                            time.sleep(1.5)

                            # Check if listbox appeared
                            try:
                                listbox = self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[@role='listbox']")))
                                time.sleep(0.5)

                                # Get visible options
                                options = listbox.find_elements(By.XPATH, ".//div[contains(@class, 'cl-text')]")
                                option_texts = [opt.text.strip() for opt in options[:10] if opt.text.strip()]
                                logger.debug(f"Dropdown {idx} options: {option_texts}")

                                # Check if this dropdown contains expected fund types
                                if any(ft in option_texts for ft in expected_fund_types):
                                    logger.info(f"✓ Found correct 펀드유형 dropdown at index {idx} with options: {option_texts}")
                                    correct_dropdown = dropdown

                                    # Close the dropdown
                                    self.driver.execute_script("arguments[0].click();", dropdown)
                                    time.sleep(1)
                                    break
                                else:
                                    logger.debug(f"Dropdown {idx} does NOT contain expected fund types, closing...")
                                    # Close the dropdown
                                    self.driver.execute_script("arguments[0].click();", dropdown)
                                    time.sleep(1)
                            except:
                                logger.debug(f"Could not open/check dropdown {idx}")
                                pass
                    except Exception as e:
                        logger.debug(f"Error checking dropdown {idx}: {e}")
                        continue

                if not correct_dropdown:
                    logger.error("Could not find the correct 펀드유형 dropdown! Falling back to original locators...")
                    fund_type_button_locators = [
                        (By.XPATH, "//div[@title='펀드유형']//div[@role='combobox']"),
                        (By.XPATH, "//div[@title='펀드유형']"),
                    ]
                else:
                    # Use the verified dropdown
                    fund_type_button_locators = [(By.XPATH, f"(//div[@role='combobox'])[{all_dropdowns.index(correct_dropdown) + 1}]")]

                self._select_from_custom_dropdown(fund_type_button_locators, fund_type_korean, f"{dataset_name}_fund_type")
                self.capture_screenshot(f"{dataset_name}_after_fund_category_select")

                # Step 2: Select Region (투자지역구분) - Direct selection by aria-label
                logger.info(f"Step 2: Selecting 투자지역구분: {region_korean}...")
                try:
                    # CRITICAL: Wait for placeholders to be replaced by actual form elements
                    logger.info("Waiting for form placeholders to be replaced with actual controls...")
                    max_wait = 20
                    form_ready = False
                    for wait_attempt in range(max_wait):
                        placeholders = self.driver.find_elements(By.XPATH, "//div[@class='cl-form-placeholder']")
                        region_radios = self.driver.find_elements(By.XPATH, f"//span[@role='radio' and @aria-label='{region_korean}']")

                        # Check if any radio buttons are visible
                        visible_radios = [r for r in region_radios if r.is_displayed()]

                        logger.debug(f"[Wait {wait_attempt+1}/{max_wait}] Placeholders: {len(placeholders)}, Radio buttons: {len(region_radios)}, Visible: {len(visible_radios)}")

                        if visible_radios and len(placeholders) < 10:  # Arbitrary threshold
                            form_ready = True
                            logger.info(f"✓ Form ready after {wait_attempt+1} seconds (found {len(visible_radios)} visible radio buttons)")
                            break

                        time.sleep(1)

                    if not form_ready:
                        logger.warning("⚠ Form may not be fully loaded, but proceeding anyway...")

                    # Additional stabilization wait
                    time.sleep(2)

                    # Find all radio buttons with the matching aria-label
                    region_radios = self.driver.find_elements(By.XPATH, f"//span[@role='radio' and @aria-label='{region_korean}']")
                    logger.debug(f"[Radio] Found {len(region_radios)} radio buttons with label '{region_korean}'")

                    if region_radios:
                        # For region, we expect only one match per label (전체, 국내, 해외, etc.)
                        # But if there are multiple, check for the one in a radiogroup with siblings containing region-related text
                        target_radio = None
                        for idx, radio in enumerate(region_radios):
                            try:
                                # CRITICAL: Only consider VISIBLE radio buttons
                                if not radio.is_displayed():
                                    logger.debug(f"[Radio] Radio button {idx+1} is not visible, skipping")
                                    continue

                                parent = radio.find_element(By.XPATH, "./ancestor::*[@role='radiogroup'][1]")
                                sibling_radios = parent.find_elements(By.XPATH, ".//span[@role='radio']")
                                sibling_labels = [r.get_attribute('aria-label') for r in sibling_radios]
                                logger.debug(f"[Radio] Radio button {idx+1} (visible) siblings: {sibling_labels}")

                                # Check if siblings contain region-related terms
                                region_terms = ['전체', '국내', '해외', '해외30', '해외60']
                                if any(term in sibling_labels for term in region_terms):
                                    # Verify this is likely the region group (should have multiple region options)
                                    if len([s for s in sibling_labels if s in region_terms]) >= 2:
                                        target_radio = radio
                                        logger.info(f"[Radio] Found 투자지역구분 '{region_korean}' button (index {idx+1}, VISIBLE)")
                                        break
                            except Exception as e:
                                logger.debug(f"[Radio] Could not check siblings for radio {idx+1}: {e}")
                                continue

                        # Fallback: use first VISIBLE match
                        if not target_radio:
                            visible_radios = [r for r in region_radios if r.is_displayed()]
                            if visible_radios:
                                target_radio = visible_radios[0]
                                logger.info(f"[Radio] Using first VISIBLE '{region_korean}' button as fallback")
                            else:
                                raise Exception(f"No visible '{region_korean}' radio buttons found")

                        aria_checked_before = target_radio.get_attribute('aria-checked')
                        logger.debug(f"[Radio] 투자지역구분 '{region_korean}' state BEFORE click: aria-checked={aria_checked_before}")

                        # Try multiple click methods to ensure the click registers
                        clicked = False

                        # Method 1: Scroll and JavaScript click
                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", target_radio)
                        time.sleep(1)

                        try:
                            self.driver.execute_script("arguments[0].click();", target_radio)
                            time.sleep(1)

                            # Verify the click worked
                            aria_checked_after = target_radio.get_attribute('aria-checked')
                            if aria_checked_after == 'true':
                                clicked = True
                                logger.debug(f"[Radio] ✓ JavaScript click succeeded")
                        except Exception as e:
                            logger.debug(f"[Radio] JavaScript click failed: {e}")

                        # Method 2: Try clicking the label element
                        if not clicked:
                            try:
                                label = target_radio.find_element(By.XPATH, "./following-sibling::label | ./parent::*/label")
                                self.driver.execute_script("arguments[0].click();", label)
                                time.sleep(1)

                                aria_checked_after = target_radio.get_attribute('aria-checked')
                                if aria_checked_after == 'true':
                                    clicked = True
                                    logger.debug(f"[Radio] ✓ Label click succeeded")
                            except Exception as e:
                                logger.debug(f"[Radio] Label click failed: {e}")

                        # Method 3: Try ActionChains physical click
                        if not clicked:
                            try:
                                actions.move_to_element(target_radio).pause(0.3).click().perform()
                                time.sleep(1)

                                aria_checked_after = target_radio.get_attribute('aria-checked')
                                if aria_checked_after == 'true':
                                    clicked = True
                                    logger.debug(f"[Radio] ✓ ActionChains click succeeded")
                            except Exception as e:
                                logger.debug(f"[Radio] ActionChains click failed: {e}")

                        # Final check
                        aria_checked_after = target_radio.get_attribute('aria-checked')
                        logger.debug(f"[Radio] 투자지역구분 '{region_korean}' state AFTER all attempts: aria-checked={aria_checked_after}")

                        if aria_checked_after == 'true':
                            logger.info(f"✓ Selected 투자지역구분: {region_korean}")
                        else:
                            logger.warning(f"⚠ 투자지역구분 '{region_korean}' click may not have registered (aria-checked={aria_checked_after})")
                    else:
                        raise Exception(f"Could not find 투자지역구분 '{region_korean}' radio button")

                except Exception as e:
                    logger.error(f"Error selecting 투자지역구분: {e}")
                    self.capture_screenshot(f"{dataset_name}_region_select_error")
                    raise

                time.sleep(random.uniform(1, 2))
                self.capture_screenshot(f"{dataset_name}_after_region_select")

                # Step 3: Select Fund Category (펀드종류) - Explicitly select "전체" (All)
                logger.info("Step 3: Selecting 펀드종류: 전체...")
                fund_category_button_locators = [
                    (By.XPATH, "//div[@title='펀드종류']//div[@role='combobox']"),
                    (By.XPATH, "//div[@title='펀드종류']"),
                ]
                self._select_from_custom_dropdown(fund_category_button_locators, "전체", f"{dataset_name}_fund_category")
                logger.info("✓ Selected 펀드종류: 전체")
                time.sleep(random.uniform(1, 2))

                # Step 4: Select Public/Private (공모/사모구분) - Click "전체" (All)
                # IMPORTANT: There are multiple "전체" radio buttons on the page.
                # The 공모/사모구분 section may have a checkbox-style interface rather than pure radio buttons
                logger.info("Step 4: Selecting 공모/사모구분: 전체...")

                # Save debug HTML to analyze structure
                debug_html_path = os.path.join(LOG_DIR, f"debug_공모사모구분_{dataset_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.html")
                try:
                    with open(debug_html_path, 'w', encoding='utf-8') as f:
                        f.write(self.driver.page_source)
                    logger.info(f"DEBUG: Saved page source to {debug_html_path}")
                except Exception as e:
                    logger.warning(f"Could not save debug HTML: {e}")

                # Debug: Print all "전체" radio buttons and their properties
                try:
                    all_jeonche = self.driver.find_elements(By.XPATH, "//span[@role='radio' and @aria-label='전체']")
                    logger.info(f"DEBUG: Found {len(all_jeonche)} total '전체' radio buttons on page:")
                    for i, radio in enumerate(all_jeonche):
                        try:
                            aria_checked = radio.get_attribute('aria-checked')
                            is_visible = radio.is_displayed()
                            is_enabled = radio.is_enabled()
                            parent = radio.find_element(By.XPATH, "./ancestor::*[@role='radiogroup'][1]")
                            siblings = parent.find_elements(By.XPATH, ".//span[@role='radio']")
                            sibling_labels = [s.get_attribute('aria-label') for s in siblings]
                            logger.info(f"  [{i+1}] aria-checked={aria_checked}, visible={is_visible}, enabled={is_enabled}, siblings={sibling_labels}")
                        except Exception as e:
                            logger.debug(f"  [{i+1}] Could not get details: {e}")
                except Exception as e:
                    logger.warning(f"DEBUG: Could not enumerate all radio buttons: {e}")

                try:
                    # Find the correct '전체' button by checking sibling labels AND visibility
                    # CRITICAL: There are hidden duplicate buttons in the DOM, we must skip those
                    target_radio = None
                    all_jeonche_radios = self.driver.find_elements(By.XPATH, "//span[@role='radio' and @aria-label='전체']")
                    logger.debug(f"[Radio] Found {len(all_jeonche_radios)} radio buttons with label '전체'")

                    for idx, radio in enumerate(all_jeonche_radios):
                        try:
                            # Check if this radio button has siblings with aria-label containing "공모" or "사모"
                            parent = radio.find_element(By.XPATH, "./ancestor::*[@role='radiogroup'][1]")
                            sibling_radios = parent.find_elements(By.XPATH, ".//span[@role='radio']")
                            sibling_labels = [r.get_attribute('aria-label') for r in sibling_radios]
                            logger.debug(f"[Radio] Radio button {idx+1} siblings: {sibling_labels}")

                            if '공모' in sibling_labels or '사모' in sibling_labels:
                                # CRITICAL: Verify this is VISIBLE and enabled
                                # There are hidden duplicate buttons in the DOM, we must skip those
                                if radio.is_displayed() and radio.is_enabled():
                                    target_radio = radio
                                    logger.info(f"[Radio] Found 공모/사모구분 '전체' button (index {idx+1}, VISIBLE and enabled)")
                                    break
                                else:
                                    logger.debug(f"[Radio] Radio button {idx+1} matches siblings but is NOT VISIBLE (visible={radio.is_displayed()}), skipping")
                        except Exception as e:
                            logger.debug(f"[Radio] Could not check siblings for radio {idx+1}: {e}")
                            continue

                    if target_radio:
                        aria_checked_before = target_radio.get_attribute('aria-checked')
                        is_visible = target_radio.is_displayed()
                        is_enabled = target_radio.is_enabled()
                        logger.debug(f"[Radio] 공모/사모구분 '전체' state: aria-checked={aria_checked_before}, visible={is_visible}, enabled={is_enabled}")

                        # Scroll into view and wait
                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", target_radio)
                        time.sleep(1)

                        # Try multiple click strategies
                        clicked = False

                        # Try 1: Click the input element inside the span
                        try:
                            input_element = target_radio.find_element(By.XPATH, ".//input[@type='checkbox' or @type='radio']")
                            self.driver.execute_script("arguments[0].click();", input_element)
                            logger.debug("[Radio] Clicked using input element inside span")
                            clicked = True
                        except:
                            pass

                        # Try 2: Click the span itself
                        if not clicked:
                            try:
                                self.driver.execute_script("arguments[0].click();", target_radio)
                                logger.debug("[Radio] Clicked using JavaScript on span")
                                clicked = True
                            except:
                                pass

                        # Try 3: Try to find and click a label element
                        if not clicked:
                            try:
                                label = target_radio.find_element(By.XPATH, "./following-sibling::label | ./parent::*/label")
                                self.driver.execute_script("arguments[0].click();", label)
                                logger.debug("[Radio] Clicked using label element")
                                clicked = True
                            except:
                                pass

                        time.sleep(1)

                        # Verify the selection
                        aria_checked_after = target_radio.get_attribute('aria-checked')
                        logger.debug(f"[Radio] 공모/사모구분 '전체' state AFTER click: aria-checked={aria_checked_after}")

                        # Take a screenshot to verify
                        self.capture_screenshot(f"{dataset_name}_after_public_private_select")

                        if aria_checked_after == 'true':
                            logger.info("✓ Selected 공모/사모구분: 전체")
                        else:
                            logger.warning(f"공모/사모구분 '전체' may not be selected (aria-checked={aria_checked_after})")
                    else:
                        raise Exception("Could not find 공모/사모구분 '전체' radio button")

                except Exception as e:
                    logger.error(f"Error selecting 공모/사모구분: {e}")
                    self.capture_screenshot(f"{dataset_name}_public_private_select_error")
                    raise

                time.sleep(random.uniform(1, 2))

                # Step 5: Select Monthly/Yearly (월간/년간(기준)) - Direct selection
                logger.info("Step 5: Selecting 월간/년간(기준): 월간...")
                try:
                    # Find all radio buttons with aria-label="월간"
                    monthly_radios = self.driver.find_elements(By.XPATH, "//span[@role='radio' and @aria-label='월간']")
                    logger.debug(f"[Radio] Found {len(monthly_radios)} radio buttons with label '월간'")

                    if monthly_radios:
                        # For monthly/yearly, we expect only one "월간" button
                        # But verify by checking siblings contain "년간" or other period options
                        target_radio = None
                        for idx, radio in enumerate(monthly_radios):
                            try:
                                parent = radio.find_element(By.XPATH, "./ancestor::*[@role='radiogroup'][1]")
                                sibling_radios = parent.find_elements(By.XPATH, ".//span[@role='radio']")
                                sibling_labels = [r.get_attribute('aria-label') for r in sibling_radios]
                                logger.debug(f"[Radio] Radio button {idx+1} siblings: {sibling_labels}")

                                # Check if siblings contain period-related terms
                                if '년간' in sibling_labels or '분기' in sibling_labels:
                                    target_radio = radio
                                    logger.info(f"[Radio] Found 월간/년간(기준) '월간' button (index {idx+1})")
                                    break
                            except Exception as e:
                                logger.debug(f"[Radio] Could not check siblings for radio {idx+1}: {e}")
                                continue

                        # Fallback: use first match
                        if not target_radio:
                            target_radio = monthly_radios[0]
                            logger.info(f"[Radio] Using first '월간' button as fallback")

                        aria_checked_before = target_radio.get_attribute('aria-checked')
                        logger.debug(f"[Radio] 월간/년간(기준) '월간' state BEFORE click: aria-checked={aria_checked_before}")

                        self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'instant', block: 'center'});", target_radio)
                        time.sleep(0.5)
                        self.driver.execute_script("arguments[0].click();", target_radio)
                        time.sleep(0.5)

                        aria_checked_after = target_radio.get_attribute('aria-checked')
                        logger.debug(f"[Radio] 월간/년간(기준) '월간' state AFTER click: aria-checked={aria_checked_after}")
                        logger.info("✓ Selected 월간/년간(기준): 월간")
                    else:
                        raise Exception("Could not find 월간/년간(기준) '월간' radio button")

                except Exception as e:
                    logger.error(f"Error selecting 월간/년간(기준): {e}")
                    self.capture_screenshot(f"{dataset_name}_monthly_yearly_select_error")
                    raise

                time.sleep(random.uniform(1, 2))

                # IMPORTANT: Wait for form to fully stabilize and DOM validation to complete
                # The site has DOM detection features, so we need to give it time to process
                logger.info("Waiting 5 seconds for form validation and DOM to stabilize...")
                time.sleep(5)

                # Step 6: Click Search/Query button (조회)
                logger.info("Step 6: Clicking Search (조회) button...")

                # Wait for form to stabilize before clicking search
                logger.info("Waiting for form to stabilize before search...")
                time.sleep(2)

                # Find the search button - need to find the visible one
                search_button_locators = [
                    (By.XPATH, "//a[@role='button' and .//div[text()='조회']]"),
                    (By.XPATH, "//div[contains(@class, 'cl-button')]//div[text()='조회']/ancestor::a[@role='button']"),
                    (By.XPATH, "//div[text()='조회']/ancestor::a[@role='button']"),
                ]

                # Find all matching buttons and pick the visible one
                search_button = None
                logger.info("Looking for visible search button...")
                for by_strategy, locator_value in search_button_locators:
                    try:
                        buttons = self.driver.find_elements(by_strategy, locator_value)
                        logger.debug(f"Found {len(buttons)} buttons matching locator: {locator_value}")

                        for idx, btn in enumerate(buttons):
                            is_displayed = btn.is_displayed()
                            logger.debug(f"  Button {idx+1}: displayed={is_displayed}, text='{btn.text}'")

                            if is_displayed:
                                search_button = btn
                                logger.info(f"✓ Found visible search button (button {idx+1})")
                                break

                        if search_button:
                            break
                    except Exception as e:
                        logger.debug(f"Locator {locator_value} failed: {e}")
                        continue

                if not search_button:
                    logger.error("Could not find visible search button!")
                    self.capture_screenshot(f"{dataset_name}_no_visible_search_button")
                    raise NoSuchElementException("No visible search button found")

                # Scroll the entire form area into view first, then scroll to button
                logger.info("Scrolling form area into view...")
                try:
                    # Find the form container
                    form_container = self.driver.find_element(By.XPATH, "//div[contains(@class, 'cl-formlayout') or contains(@class, 'cl-container')]")
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'start'});", form_container)
                    time.sleep(2)
                except:
                    logger.debug("Could not scroll form container")

                # Now scroll button into view with aggressive scrolling
                logger.info("Scrolling search button into view...")
                self.driver.execute_script("""
                    var element = arguments[0];
                    element.scrollIntoView({behavior: 'instant', block: 'center', inline: 'center'});
                    window.scrollBy(0, -100);
                """, search_button)
                time.sleep(2)

                # Verify it's now visible
                is_visible = search_button.is_displayed()
                logger.info(f"Search button visible after scrolling: {is_visible}")

                if not is_visible:
                    logger.warning("Button still not visible after scrolling, trying alternative scroll...")
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", search_button)
                    time.sleep(1)
                    # Check again
                    is_visible = search_button.is_displayed()
                    logger.info(f"Button visible after alternative scroll: {is_visible}")

                # Take screenshot before clicking
                self.capture_screenshot(f"{dataset_name}_before_search_click")

                # Get initial page state to verify data loads
                initial_grid_html = ""
                try:
                    grid_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'cl-grid')]")
                    initial_grid_html = grid_element.get_attribute('innerHTML')[:500]
                    logger.debug(f"Initial grid HTML length: {len(initial_grid_html)}")
                except:
                    logger.debug("Could not get initial grid state")

                # Physical mouse click - try the anchor tag inside the button first
                clicked = False
                try:
                    # Try to find the clickable <a> tag inside the button
                    anchor_link = search_button.find_element(By.XPATH, ".//a[@role='button']")
                    logger.debug("Found clickable anchor link inside search button")

                    # Physical mouse move and click with ActionChains
                    actions.move_to_element(anchor_link).pause(0.5).click().pause(0.3).perform()
                    logger.info("✓ Clicked Search button anchor using physical ActionChains click")
                    clicked = True
                except Exception as e:
                    logger.debug(f"Anchor link click failed: {e}")

                # Fallback: Click the button container itself
                if not clicked:
                    try:
                        actions.move_to_element(search_button).pause(0.5).click().pause(0.3).perform()
                        logger.info("✓ Clicked Search button container using physical ActionChains click")
                        clicked = True
                    except Exception as e:
                        logger.debug(f"Button container click failed: {e}")

                # Try clicking with offset (click center of element)
                if not clicked:
                    try:
                        actions.move_to_element_with_offset(search_button, 0, 0).pause(0.3).click().perform()
                        logger.info("✓ Clicked Search button using offset click")
                        clicked = True
                    except Exception as e:
                        logger.debug(f"Offset click failed: {e}")

                # Last resort: Direct element click
                if not clicked:
                    try:
                        search_button.click()
                        logger.info("✓ Clicked Search button using direct click")
                        clicked = True
                    except Exception as e:
                        logger.error(f"All search button click methods failed: {e}")
                        self.capture_screenshot(f"{dataset_name}_search_button_click_failed")
                        # Log what element we found
                        try:
                            logger.error(f"Button tag: {search_button.tag_name}, text: {search_button.text}")
                            logger.error(f"Button displayed: {search_button.is_displayed()}, enabled: {search_button.is_enabled()}")
                        except:
                            pass
                        raise

                logger.info("Waiting for search to process and results to load...")
                time.sleep(3)

                # Wait for data to actually change (verify search executed)
                data_loaded = False
                max_wait = 15
                for i in range(max_wait):
                    try:
                        # Check if grid content has changed
                        grid_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'cl-grid')]")
                        current_grid_html = grid_element.get_attribute('innerHTML')[:500]

                        # Look for data indicators: row count text "[총 XX건]" or actual data rows
                        row_count_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), '총') and contains(text(), '건')]")
                        data_rows = self.driver.find_elements(By.XPATH, "//div[@role='row' and @aria-rowindex]")

                        if len(data_rows) > 2:  # More than just header rows
                            data_loaded = True
                            logger.info(f"✓ Search results loaded: Found {len(data_rows)} data rows after {i+1} seconds")
                            break
                        elif row_count_elements:
                            for elem in row_count_elements:
                                text = elem.text
                                if "총" in text and "건" in text:
                                    logger.info(f"✓ Search results loaded: {text} after {i+1} seconds")
                                    data_loaded = True
                                    break
                            if data_loaded:
                                break

                        logger.debug(f"Waiting for data... ({i+1}/{max_wait}s, rows: {len(data_rows)})")
                        time.sleep(1)
                    except Exception as e:
                        logger.debug(f"Error checking data load status: {e}")
                        time.sleep(1)

                if not data_loaded:
                    logger.warning("⚠ Could not verify that search results loaded - DOM may have detected bot")
                    self.capture_screenshot(f"{dataset_name}_search_no_data_loaded")
                    # Save page source for debugging
                    try:
                        debug_html_path = os.path.join(LOG_DIR, f"debug_search_no_data_{dataset_name}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.html")
                        with open(debug_html_path, 'w', encoding='utf-8') as f:
                            f.write(self.driver.page_source)
                        logger.error(f"DEBUG: Page source after search saved to {debug_html_path}")
                    except Exception as e:
                        logger.warning(f"Could not save debug page source: {e}")

                # Final wait to ensure everything is stable
                time.sleep(2)
                self.capture_screenshot(f"{dataset_name}_after_search")

                initial_files = set(os.listdir(DOWNLOAD_DIR))

                # Step 7: Locate and click Excel download icon
                logger.info("Step 7: Clicking Excel download button...")

                # Wait for Excel button to appear and become enabled
                logger.info("Waiting for Excel download button to be ready...")
                time.sleep(2)

                # Find Excel button - look for the specific icon with excel image
                # The button structure is: <a role='button'> -> <div class='cl-icon' style='background-image:...excel...'>
                excel_button_locators = [
                    # Look for the anchor tag that contains the excel icon
                    (By.XPATH, "//a[@role='button' and .//div[contains(@style, 'icon-file-excel')]]"),
                    (By.XPATH, "//a[@role='button' and @title='EXCEL저장']"),
                    (By.XPATH, "//div[contains(@style, 'icon-file-excel')]/ancestor::a[@role='button']"),
                    # Fallback to just the icon
                    (By.XPATH, "//div[contains(@class, 'cl-icon') and contains(@style, 'icon-file-excel')]"),
                ]

                # Find all matching buttons and pick the visible one
                excel_button = None
                logger.info("Looking for visible Excel download button...")
                for by_strategy, locator_value in excel_button_locators:
                    try:
                        buttons = self.driver.find_elements(by_strategy, locator_value)
                        logger.debug(f"Found {len(buttons)} Excel buttons matching locator: {locator_value}")

                        for idx, btn in enumerate(buttons):
                            is_displayed = btn.is_displayed()
                            tag_name = btn.tag_name
                            logger.debug(f"  Excel Button {idx+1}: tag={tag_name}, displayed={is_displayed}")

                            if is_displayed:
                                excel_button = btn
                                logger.info(f"✓ Found visible Excel button (button {idx+1}, tag={tag_name})")
                                break

                        if excel_button:
                            break
                    except Exception as e:
                        logger.debug(f"Locator {locator_value} failed: {e}")
                        continue

                if not excel_button:
                    logger.error("Could not find visible Excel button!")
                    self.capture_screenshot(f"{dataset_name}_no_visible_excel_button")
                    raise NoSuchElementException("No visible Excel button found")

                # Scroll Excel button into view with aggressive scrolling
                logger.info("Scrolling Excel button into view...")
                self.driver.execute_script("""
                    var element = arguments[0];
                    element.scrollIntoView({behavior: 'instant', block: 'center', inline: 'center'});
                    window.scrollBy(0, -50);
                """, excel_button)
                time.sleep(1.5)

                # Verify it's now visible
                is_visible = excel_button.is_displayed()
                logger.info(f"Excel button visible after scrolling: {is_visible}")

                if not is_visible:
                    logger.warning("Excel button still not visible after scrolling, trying alternative scroll...")
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", excel_button)
                    time.sleep(1)
                    is_visible = excel_button.is_displayed()
                    logger.info(f"Excel button visible after alternative scroll: {is_visible}")

                self.capture_screenshot(f"{dataset_name}_before_excel_click")

                # Physical mouse click on the Excel button
                clicked = False

                # Try 1: If we found an anchor tag, click it with ActionChains (physical click)
                if excel_button.tag_name == 'a':
                    try:
                        actions.move_to_element(excel_button).pause(0.5).click().pause(0.3).perform()
                        logger.info("✓ Clicked Excel anchor button using physical ActionChains click")
                        clicked = True
                    except Exception as e:
                        logger.debug(f"Anchor ActionChains click failed: {e}")

                # Try 2: If it's a div (icon), find the parent anchor and click that
                if not clicked and excel_button.tag_name == 'div':
                    try:
                        parent_anchor = excel_button.find_element(By.XPATH, "./ancestor::a[@role='button']")
                        logger.debug("Found parent anchor for Excel icon")
                        actions.move_to_element(parent_anchor).pause(0.5).click().pause(0.3).perform()
                        logger.info("✓ Clicked Excel parent anchor using physical ActionChains click")
                        clicked = True
                    except Exception as e:
                        logger.debug(f"Parent anchor click failed: {e}")

                # Try 3: JavaScript click on the element
                if not clicked:
                    try:
                        self.driver.execute_script("arguments[0].click();", excel_button)
                        logger.info("✓ Clicked Excel button using JavaScript")
                        clicked = True
                    except Exception as e:
                        logger.debug(f"JavaScript click failed: {e}")

                # Try 4: Click using ActionChains with offset
                if not clicked:
                    try:
                        actions.move_to_element_with_offset(excel_button, 0, 0).pause(0.3).click().perform()
                        logger.info("✓ Clicked Excel button using offset click")
                        clicked = True
                    except Exception as e:
                        logger.debug(f"Offset click failed: {e}")

                # Try 5: Direct click (last resort)
                if not clicked:
                    try:
                        excel_button.click()
                        logger.info("✓ Clicked Excel button using direct click")
                        clicked = True
                    except Exception as e:
                        logger.error(f"All Excel button click methods failed: {e}")
                        self.capture_screenshot(f"{dataset_name}_excel_button_click_failed")
                        # Log what element we found
                        try:
                            logger.error(f"Excel button tag: {excel_button.tag_name}, displayed: {excel_button.is_displayed()}, enabled: {excel_button.is_enabled()}")
                        except:
                            pass
                        raise

                logger.info("Waiting 5 seconds for download to initiate...")
                time.sleep(5)
                self.capture_screenshot(f"{dataset_name}_after_excel_click")

                downloaded_file_path = self.wait_for_download_completion(initial_files)
                self.capture_screenshot(f"{dataset_name}_after_download_complete")

                if downloaded_file_path:
                    new_file_name = f"{output_name}.xls"
                    new_file_path = os.path.join(DOWNLOAD_DIR, new_file_name)

                    # If file already exists, delete it first
                    if os.path.exists(new_file_path):
                        logger.info(f"File {new_file_path} already exists, deleting it first...")
                        os.remove(new_file_path)

                    os.rename(downloaded_file_path, new_file_path)
                    self.downloaded_files.append(new_file_path)
                    logger.info(f"Successfully downloaded and renamed to: {new_file_path}")
                    success = True

                    # IMPORTANT: After successful download, wait for any popups/alerts to close
                    # and for the page to return to normal state before next dataset
                    logger.info("Waiting for page to stabilize after download...")
                    time.sleep(3)

                    # Check for and close any alert popups
                    try:
                        alert = self.driver.switch_to.alert
                        alert_text = alert.text
                        logger.info(f"Alert popup detected: {alert_text}")
                        alert.accept()
                        logger.info("Alert popup closed")
                        time.sleep(2)
                    except:
                        logger.debug("No alert popup detected")

                    # Switch back to main frame in case we got switched out
                    try:
                        self.driver.switch_to.default_content()
                        self.driver.switch_to.frame("main")
                        logger.debug("Switched back to main frame after download")
                    except Exception as e:
                        logger.debug(f"Frame switch after download: {e}")

                    # Wait a bit more for DOM to settle
                    time.sleep(2)

                    break
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
                break
        return success

    def process_excel_file(self, file_path, dataset_config):
        """Processes a single downloaded Excel file."""
        logger.info(f"Processing file: {file_path}")
        output_name_prefix = dataset_config['output_name']
        try:
            df = pd.read_excel(file_path, header=[2, 3], engine='openpyxl')
            new_columns = []
            for col_level_0, col_level_1 in df.columns:
                if col_level_0 == '기준일':
                    new_columns.append('Date')
                else:
                    category = KOREAN_TO_ENGLISH_CATEGORY.get(col_level_0.strip(), col_level_0.strip())
                    metric = KOREAN_TO_ENGLISH_METRIC.get(col_level_1.strip(), col_level_1.strip())
                    col_name = f"{output_name_prefix}: {category}: {metric}"
                    new_columns.append(col_name)
                    if col_name not in TIMESERIES_CODES:
                        TIMESERIES_CODES[col_name] = f"KMFK_{self.timeseries_counter:04d}"
                        self.timeseries_counter += 1
            df.columns = new_columns
            logger.info(f"Columns renamed for {output_name_prefix}.")
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df['Date'] = df['Date'].dt.strftime('%Y-%m')
            logger.info("Dates formatted to YYYY-MM.")
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
        """Combines all processed data into DATA and METADATA files and creates a ZIP archive."""
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
        try:
            merged_df = self.all_processed_data[0]
            for df in self.all_processed_data[1:]:
                merged_df = pd.merge(merged_df, df, on='Date', how='outer')
            merged_df = merged_df.sort_values('Date')
            codes = ['Date'] + [TIMESERIES_CODES.get(col, col) for col in merged_df.columns if col != 'Date']
            descriptions = ['Date'] + [col for col in merged_df.columns if col != 'Date']
            final_df = pd.DataFrame([codes, descriptions], columns=merged_df.columns)
            final_df = pd.concat([final_df, merged_df], ignore_index=True)
            final_df.to_excel(data_filepath, index=False, header=False, engine='openpyxl')
            logger.info(f"DATA file created: {data_filepath}")
        except Exception as e:
            logger.error(f"Error generating DATA file: {e}", exc_info=True)
            return
        try:
            today = datetime.datetime.now()
            if today.month == 12:
                next_month = datetime.datetime(today.year + 1, 1, 1)
            else:
                next_month = datetime.datetime(today.year, today.month + 1, 1)
            if next_month.month == 12:
                last_day = datetime.datetime(next_month.year + 1, 1, 1) - datetime.timedelta(days=1)
            else:
                last_day = datetime.datetime(next_month.year, next_month.month + 1, 1) - datetime.timedelta(days=1)
            next_release_date = last_day.strftime("%Y-%m-%dT12:00:00")
            metadata_rows = []
            for col in merged_df.columns:
                if col != 'Date':
                    code = TIMESERIES_CODES.get(col, col)
                    metadata_rows.append({
                        'CODE': code, 'DESCRIPTION': col, 'FREQUENCY': 'Monthly',
                        'UNIT': 'KRW Million' if 'Amount' in col else 'Percentage',
                        'NEXT_RELEASE_DATE': next_release_date
                    })
            metadata_df = pd.DataFrame(metadata_rows)
            metadata_df.to_excel(metadata_filepath, index=False, engine='openpyxl')
            logger.info(f"METADATA file created: {metadata_filepath}")
        except Exception as e:
            logger.error(f"Error generating METADATA file: {e}", exc_info=True)
            return
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
        """Main execution method that runs the entire scraping workflow."""
        start_time = time.time()
        success_count = 0
        failure_count = 0
        total_datasets = len(DATASET_CONFIGS)

        try:
            logger.info("=" * 70)
            logger.info("Starting KOFIA Web Scraper")
            logger.info(f"Start Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Total Datasets to Process: {total_datasets}")
            logger.info("=" * 70)

            nav_start = time.time()
            logger.info("STEP 1/3: Navigating to fund composition query page...")
            self.navigate_to_fund_composition_query()
            nav_duration = time.time() - nav_start
            logger.info(f"✓ Navigation completed in {nav_duration:.2f} seconds")

            logger.info("=" * 70)
            logger.info("STEP 2/3: Downloading datasets...")
            for idx, config in enumerate(DATASET_CONFIGS, 1):
                dataset_start = time.time()
                logger.info(f"Processing Dataset [{idx}/{total_datasets}]: {config['name']}")
                logger.info(f"  - Fund Type: {config['fund_type_korean']}, Region: {config['region_korean']}")

                if self.download_dataset(config):
                    success_count += 1
                    dataset_duration = time.time() - dataset_start
                    logger.info(f"✓ Dataset {idx}/{total_datasets} completed in {dataset_duration:.2f}s")
                else:
                    failure_count += 1
                    dataset_duration = time.time() - dataset_start
                    logger.warning(f"✗ Dataset {idx}/{total_datasets} failed after {dataset_duration:.2f}s")

                logger.info(f"Progress: {idx}/{total_datasets} datasets processed ({success_count} successful, {failure_count} failed)")
                logger.info("-" * 70)

            download_duration = time.time() - nav_start - nav_duration
            logger.info(f"✓ All downloads completed in {download_duration:.2f} seconds")
            logger.info(f"Download Summary: {success_count} successful, {failure_count} failed out of {total_datasets} total")

            # PROCESSING DISABLED - Only downloading files
            logger.info("=" * 70)
            logger.info("STEP 3/3: Processing SKIPPED (only downloading files)")
            logger.info(f"✓ Downloaded {len(self.downloaded_files)} files successfully")

            # # PROCESSING CODE COMMENTED OUT
            # logger.info("STEP 3/3: Processing downloaded files and generating output...")
            # process_start = time.time()
            # for idx, (file_path, config) in enumerate(zip(self.downloaded_files, DATASET_CONFIGS), 1):
            #     logger.info(f"Processing file [{idx}/{len(self.downloaded_files)}]: {os.path.basename(file_path)}")
            #     self.process_excel_file(file_path, config)
            # self.generate_output_files()
            # process_duration = time.time() - process_start
            # logger.info(f"✓ Processing completed in {process_duration:.2f} seconds")

            # Final Summary
            total_duration = time.time() - start_time
            logger.info("=" * 70)
            logger.info("EXECUTION SUMMARY")
            logger.info("=" * 70)
            logger.info(f"End Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Total Execution Time: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
            logger.info(f"Navigation Time: {nav_duration:.2f}s")
            logger.info(f"Download Time: {download_duration:.2f}s")
            logger.info(f"Processing Time: {process_duration:.2f}s")
            logger.info(f"Datasets Processed: {total_datasets}")
            logger.info(f"  - Successful: {success_count}")
            logger.info(f"  - Failed: {failure_count}")
            logger.info(f"Files Downloaded: {len(self.downloaded_files)}")
            logger.info(f"Timeseries Created: {self.timeseries_counter - 1}")
            logger.info("=" * 70)
            logger.info("✓ KOFIA Web Scraper completed successfully")
            logger.info("=" * 70)

        except Exception as e:
            total_duration = time.time() - start_time
            logger.error("=" * 70)
            logger.error("EXECUTION FAILED")
            logger.error("=" * 70)
            logger.error(f"Fatal error in scraper execution: {e}", exc_info=True)
            logger.error(f"Error occurred after {total_duration:.2f} seconds")
            logger.error(f"Datasets processed before error: {success_count + failure_count}/{total_datasets}")
            logger.error(f"Current URL: {self.driver.current_url if self.driver else 'Unknown'}")
            logger.error("=" * 70)
            self.capture_screenshot("fatal_error")

        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed.")


def download_datasets_batch(dataset_batch, thread_id, use_fixed_date=False, keep_alive=False):
    """
    Worker function to download a batch of datasets in a single browser tab.
    Each tab processes multiple datasets sequentially.
    Returns (results, scraper) where scraper is kept alive if keep_alive=True
    results: list of (success, dataset_name) tuples
    """
    logger_name = f"Tab_{thread_id}"
    thread_logger = logging.getLogger(logger_name)
    results = []
    scraper = None

    try:
        thread_logger.info(f"[Tab {thread_id}] Starting with {len(dataset_batch)} datasets")

        # Create ONE scraper instance for this tab
        scraper = KOFIAScraper(use_fixed_date=use_fixed_date)

        # Navigate to the page ONCE
        scraper.navigate_to_fund_composition_query()

        # Download each dataset in this batch sequentially
        for idx, dataset_config in enumerate(dataset_batch, 1):
            thread_logger.info(f"[Tab {thread_id}] Downloading {idx}/{len(dataset_batch)}: {dataset_config['name']}")

            success = scraper.download_dataset(dataset_config)

            if success:
                thread_logger.info(f"[Tab {thread_id}] ✓ SUCCESS: {dataset_config['name']}")
                results.append((True, dataset_config['name']))
            else:
                thread_logger.warning(f"[Tab {thread_id}] ✗ FAILED: {dataset_config['name']}")
                results.append((False, dataset_config['name']))

        # Cleanup (only if not keeping alive for retry)
        if not keep_alive and scraper.driver:
            scraper.driver.quit()
            thread_logger.info(f"[Tab {thread_id}] Browser closed after completing {len(dataset_batch)} datasets")
            scraper = None
        elif keep_alive:
            thread_logger.info(f"[Tab {thread_id}] Keeping browser alive for retry")

        return (results, scraper)

    except Exception as e:
        thread_logger.error(f"[Tab {thread_id}] ✗ ERROR: {e}")
        # Mark all remaining datasets as failed
        for dataset_config in dataset_batch:
            if not any(dataset_config['name'] == name for _, name in results):
                results.append((False, dataset_config['name']))

        # Cleanup on error
        if scraper and scraper.driver:
            try:
                scraper.driver.quit()
            except:
                pass

        return (results, None)


def run_parallel():
    """
    Run scraper with 3 parallel browser tabs.
    Tab 1: Datasets 1-4 (indexes 0-3)
    Tab 2: Datasets 5-8 (indexes 4-7)
    Tab 3: Datasets 9-11 (indexes 8-10)
    """
    start_time = time.time()

    # Split datasets into 3 batches
    batch_1 = DATASET_CONFIGS[0:4]   # Datasets 1-4
    batch_2 = DATASET_CONFIGS[4:8]   # Datasets 5-8
    batch_3 = DATASET_CONFIGS[8:11]  # Datasets 9-11

    logger.info("=" * 70)
    logger.info("KOFIA Parallel Web Scraper - 3 Tabs")
    logger.info("=" * 70)
    logger.info(f"Tab 1: Datasets 1-4  ({len(batch_1)} datasets)")
    logger.info(f"Tab 2: Datasets 5-8  ({len(batch_2)} datasets)")
    logger.info(f"Tab 3: Datasets 9-11 ({len(batch_3)} datasets)")
    logger.info(f"Total: {len(DATASET_CONFIGS)} datasets")
    logger.info("=" * 70)

    all_results = []
    active_scrapers = {}  # Store successful scrapers for reuse

    # Launch 3 tabs concurrently with staggered start
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit each batch to a separate thread with staggered delays to avoid race conditions
        # keep_alive=True to reuse successful tabs for retry
        future_tab1 = executor.submit(download_datasets_batch, batch_1, 1, False, True)
        time.sleep(2)  # Stagger tab starts to reduce simultaneous page loads
        future_tab2 = executor.submit(download_datasets_batch, batch_2, 2, False, True)
        time.sleep(2)
        future_tab3 = executor.submit(download_datasets_batch, batch_3, 3, False, True)

        # Wait for all tabs to complete
        logger.info("All 3 tabs running concurrently...")

        # Collect results as they complete and store successful scrapers
        future_map = {future_tab1: 1, future_tab2: 2, future_tab3: 3}
        for future in as_completed([future_tab1, future_tab2, future_tab3]):
            try:
                batch_results, scraper = future.result()
                all_results.extend(batch_results)

                # Store scraper if it's still alive (no errors)
                tab_id = future_map[future]
                if scraper and scraper.driver:
                    active_scrapers[tab_id] = scraper
                    logger.info(f"Tab {tab_id} completed successfully and kept alive. Total progress: {len(all_results)}/{len(DATASET_CONFIGS)}")
                else:
                    logger.info(f"Tab {tab_id} completed with errors. Total progress: {len(all_results)}/{len(DATASET_CONFIGS)}")
            except Exception as e:
                logger.error(f"Tab encountered error: {e}")

    # Summary
    total_duration = time.time() - start_time
    success_count = sum(1 for success, _ in all_results if success)
    failure_count = len(all_results) - success_count

    logger.info("=" * 70)
    logger.info("PARALLEL DOWNLOAD COMPLETE - FIRST ATTEMPT")
    logger.info("=" * 70)
    logger.info(f"Total Time: {total_duration:.2f} seconds")
    logger.info(f"Success: {success_count}/{len(DATASET_CONFIGS)}")
    logger.info(f"Failed: {failure_count}/{len(DATASET_CONFIGS)}")

    # Retry failed datasets using successful tabs
    if failure_count > 0:
        logger.info("=" * 70)
        logger.info("RETRYING FAILED DATASETS")
        logger.info("=" * 70)

        # Collect failed datasets
        failed_datasets = []
        for success, name in all_results:
            if not success:
                logger.info(f"  ✗ {name}")
                # Find the dataset config by name
                for config in DATASET_CONFIGS:
                    if config['name'] == name:
                        failed_datasets.append(config)
                        break

        if failed_datasets and active_scrapers:
            # Use the first successful tab for retry
            retry_tab_id = list(active_scrapers.keys())[0]
            retry_scraper = active_scrapers[retry_tab_id]

            logger.info(f"\nRetrying {len(failed_datasets)} failed datasets using Tab {retry_tab_id} (already working)...")
            retry_start = time.time()

            retry_results = []
            thread_logger = logging.getLogger(f"Tab_{retry_tab_id}_Retry")

            # Download each failed dataset using the working tab
            for idx, dataset_config in enumerate(failed_datasets, 1):
                thread_logger.info(f"[Tab {retry_tab_id} Retry] Downloading {idx}/{len(failed_datasets)}: {dataset_config['name']}")

                success = retry_scraper.download_dataset(dataset_config)

                if success:
                    thread_logger.info(f"[Tab {retry_tab_id} Retry] ✓ SUCCESS: {dataset_config['name']}")
                    retry_results.append((True, dataset_config['name']))
                else:
                    thread_logger.warning(f"[Tab {retry_tab_id} Retry] ✗ FAILED: {dataset_config['name']}")
                    retry_results.append((False, dataset_config['name']))

            # Update results
            retry_success_count = sum(1 for success, _ in retry_results if success)
            retry_duration = time.time() - retry_start

            logger.info("=" * 70)
            logger.info("RETRY COMPLETE")
            logger.info("=" * 70)
            logger.info(f"Retry Time: {retry_duration:.2f} seconds")
            logger.info(f"Retry Success: {retry_success_count}/{len(failed_datasets)}")

            # Final summary
            final_success = success_count + retry_success_count
            final_failure = len(DATASET_CONFIGS) - final_success

            logger.info("=" * 70)
            logger.info("FINAL RESULTS")
            logger.info("=" * 70)
            logger.info(f"Total Time: {(time.time() - start_time):.2f} seconds")
            logger.info(f"Final Success: {final_success}/{len(DATASET_CONFIGS)}")
            logger.info(f"Final Failed: {final_failure}/{len(DATASET_CONFIGS)}")

            if final_failure > 0:
                logger.info("\nStill failed after retry:")
                for success, name in retry_results:
                    if not success:
                        logger.info(f"  ✗ {name}")
            logger.info("=" * 70)
        elif failed_datasets and not active_scrapers:
            logger.warning("No successful tabs available for retry - all tabs had errors")
            logger.info("=" * 70)
        else:
            logger.info("=" * 70)
    else:
        logger.info("=" * 70)

    # Cleanup all active scrapers
    for tab_id, scraper in active_scrapers.items():
        try:
            if scraper.driver:
                scraper.driver.quit()
                logger.info(f"Tab {tab_id} browser closed")
        except Exception as e:
            logger.warning(f"Error closing Tab {tab_id}: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='KOFIA Mutual Fund Data Scraper')
    parser.add_argument('--fixed-date', action='store_true', help='Use fixed date 2025-03-31 for initial release')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode (not headless)')
    args = parser.parse_args()

    # FIX: When --visible is specified, set HEADLESS to False
    if args.visible:
        import config
        config.HEADLESS = False

    print("\n" + "=" * 60)
    print("KOFIA Mutual Fund Data Scraper")
    print("=" * 60)
    print(f"Browser: {BROWSER}")
    print(f"Headless: {HEADLESS_MODE}")
    print(f"Parallel Tabs: {NUM_PARALLEL_TABS}")
    print(f"Download Directory: {DOWNLOAD_DIR}")
    print(f"Log Directory: {LOG_DIR}")
    print(f"Using fixed date: {args.fixed_date}")
    print("=" * 60 + "\n")

    # Use parallel or sequential based on NUM_PARALLEL_TABS
    if NUM_PARALLEL_TABS > 1:
        logger.info(f"Running in PARALLEL mode with {NUM_PARALLEL_TABS} tabs")
        run_parallel()
    else:
        logger.info("Running in SEQUENTIAL mode (single tab)")
        scraper = KOFIAScraper(use_fixed_date=args.fixed_date)
        scraper.run()
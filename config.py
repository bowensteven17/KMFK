import os
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions

# --- General Configuration ---
BASE_URL = "http://freesis.kofia.or.kr/"
BROWSER = "chrome"  # or "firefox"
HEADLESS = False  # Stealth mode works better in non-headless
USE_STEALTH = True  # Enable undetected-chromedriver for stealth
IMPLICIT_WAIT = 10
EXPLICIT_WAIT_TIMEOUT = 30
DOWNLOAD_WAIT_TIMEOUT = 120 # Increased timeout for downloads

# --- Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Ensure directories exist
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# --- Selenium WebDriver Options ---
def get_chrome_options():
    """Get Chrome options for regular Selenium WebDriver"""
    chrome_options = ChromeOptions()
    if HEADLESS:
        chrome_options.add_argument("--headless=new")  # New headless mode
        chrome_options.add_argument("--disable-gpu")

    # Basic stealth arguments
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--start-maximized")

    # More realistic user agent
    chrome_options.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    # Download preferences
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.notifications": 2,  # Disable notifications
        "credentials_enable_service": False,  # Disable save password prompts
        "profile.password_manager_enabled": False
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    return chrome_options

def get_stealth_chrome_options():
    """Get options for undetected-chromedriver (stealth mode)"""
    options = uc.ChromeOptions()

    # Download preferences
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False
    }
    options.add_experimental_option("prefs", prefs)

    # Keep these minimal - uc.Chrome handles most stealth automatically
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    return options

def get_firefox_options():
    firefox_options = FirefoxOptions()
    if HEADLESS:
        firefox_options.add_argument("-headless")
    firefox_options.set_preference("browser.download.folderList", 2)
    firefox_options.set_preference("browser.download.manager.showWhenStarting", False)
    firefox_options.set_preference("browser.download.dir", DOWNLOAD_DIR)
    firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel, application/xls, application/octet-stream")
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0")
    return firefox_options

# --- Logging Configuration ---
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# --- Retry Mechanism ---
MAX_RETRIES = 3
RETRY_DELAY = 5 # seconds

# --- KOFIA Specific Locators and Data ---
# Main Navigation
# Step 2 from runbook: Click on the "펀드" (Fund) tab highlighted in red
# ACTUAL HTML: <li class="menu2"><a href="/stat/FreeSIS.do?parentDivId=MSIS40100000000000"><img src="..." alt="펀드"></a></li>
# The tab uses an IMAGE with alt="펀드", not plain text!
NAV_FUND_TAB = [
    ("xpath", "//img[@alt='펀드']"),  # Primary: by img alt attribute (ACTUAL structure)
    ("xpath", "//a[img[@alt='펀드']]"),  # Click the <a> that contains the img
    ("xpath", "//li[@class='menu2']//a[contains(@href, 'MSIS40100000000000')]"),  # By specific href
    ("xpath", "//a[contains(@href, 'MSIS40100000000000')]"),  # Simplified href match
    ("xpath", "//li[@class='menu2']//img[@alt='펀드']"),  # With menu2 class context
]

# Step 3 from runbook: After clicking Fund tab, navigate through the left sidebar menu
# The runbook shows a numbered sequence (1, 2, 3, 4) in the left menu
# Item 4 (highlighted in red) is "투자자산별현황" (Investment Asset Status / Fund Composition)
# This appears to be the direct link to the fund composition query page

# Multiple locator strategies for the fund composition menu item:
# Step 3: Click on "투자자산별현황" in the LEFT SIDEBAR after Fund tab is clicked
# This menu appears in the left sidebar (not the top navigation dropdown)
# May also use image tags like the main navigation
NAV_FUND_COMPOSITION_MENU = [
    ("xpath", "//img[@alt='투자자산별현황']"),  # If it uses img tag like main nav
    ("xpath", "//a[contains(text(), '투자자산별현황')]"),  # If it uses text
    ("xpath", "//a[img[@alt='투자자산별현황']]"),  # Parent link of img
    ("xpath", "//a[contains(@href, 'fundAsset')]"),  # By href pattern
    ("xpath", "//a[contains(@href, 'invAsset')]"),  # Alternative href pattern
    ("xpath", "//a[contains(@href, 'STATFND')]"),  # Generic fund stats pattern
    ("xpath", "//div[@id='leftMenu']//a[contains(text(), '투자')]"),  # With left menu context
    ("xpath", "//div[contains(@class, 'leftMenu')]//a[contains(text(), '투자')]"),  # Alternative left menu
]

# Note: Removed NAV_FUND_COMPOSITION_ALT as it's now integrated into the list above
# Note: NAV_STATISTICS_SECTION is removed as the runbook doesn't show a separate statistics section click
# The navigation appears to be: Home -> Fund Tab -> Fund Composition (투자자산별현황) directly


# Form Elements (these are placeholders and require actual inspection of the KOFIA site)
# Using more robust XPath for Korean text.
SELECT_FUND_TYPE = ("id", "fndSctCd") # Assuming dropdown has this ID
SELECT_REGION = ("id", "invDstCcd")  # Assuming dropdown has this ID
SELECT_FUND_UNIVERSE = ("id", "fndTrstCcd") # Assuming dropdown has this ID
INPUT_START_DATE = ("id", "startDate") # Assuming input has this ID
INPUT_END_DATE = ("id", "endDate")   # Assuming input has this ID
BUTTON_SEARCH = ("id", "searchBtn")  # Assuming button has this ID
BUTTON_EXCEL_DOWNLOAD = ("xpath", "//img[@alt='엑셀'] | //a[contains(@onclick, 'fn_excel') or contains(text(), '엑셀다운로드')]") # General Excel icon/button

# Korean Text for dropdown options (verify these are exact matches on the website)
# Fund Type options
FUND_TYPES = {
    'Equity Funds': '주식형',
    'Hybrid Equity': '혼합형주식',
    'Hybrid Bond': '혼합형채권',
    'Bond': '채권형',
    'Money Market': 'MMF',
    'Hybrid Asset': '혼합자산' # This is the "last category in dropdown (14th option)"
}
# Region options
REGIONS = {
    'All/Total': '전체', # Assuming '전체' is the text for All/Total
    'Domestic': '국내'
}

# Mapping dataset configurations to actual dropdown selections (index-based where text isn't unique)
# IMPORTANT: These indices are **guesses**. You MUST verify them on the live site.
# Selenium's Select class uses 0-based indexing for options.
DATASET_CONFIGS = [
    {
        'name': 'RawDataEquity',
        'fund_type_korean': FUND_TYPES['Equity Funds'],
        'fund_type_index': 1, # 2nd option, so index 1
        'region_korean': REGIONS['All/Total'],
        'region_index': 0, # First option, so index 0
        'time_window_years': 5,
        'fund_universe_index': 0, # Assuming 'Total' is first option
        'output_name': 'Equity'
    },
    {
        'name': 'RawDataDomesticEquity',
        'fund_type_korean': FUND_TYPES['Equity Funds'],
        'fund_type_index': 1,
        'region_korean': REGIONS['Domestic'],
        'region_index': 1, # Assuming Domestic is second option
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'DomesticEquity'
    },
    {
        'name': 'RawDataHybridEquity',
        'fund_type_korean': FUND_TYPES['Hybrid Equity'],
        'fund_type_index': 2, # 3rd option, so index 2
        'region_korean': REGIONS['All/Total'],
        'region_index': 0,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'HybridEquity'
    },
    {
        'name': 'RawDataHybridDomesticEquity',
        'fund_type_korean': FUND_TYPES['Hybrid Equity'],
        'fund_type_index': 2,
        'region_korean': REGIONS['Domestic'],
        'region_index': 1,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'HybridDomesticEquity'
    },
    {
        'name': 'RawDataHybridBond',
        'fund_type_korean': FUND_TYPES['Hybrid Bond'],
        'fund_type_index': 3, # 4th option, so index 3
        'region_korean': REGIONS['All/Total'],
        'region_index': 0,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'HybridBond'
    },
    {
        'name': 'RawDataHybridDomesticBond',
        'fund_type_korean': FUND_TYPES['Hybrid Bond'],
        'fund_type_index': 3,
        'region_korean': REGIONS['Domestic'],
        'region_index': 1,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'HybridDomesticBond'
    },
    {
        'name': 'RawDataBond',
        'fund_type_korean': FUND_TYPES['Bond'],
        'fund_type_index': 4, # 5th option, so index 4
        'region_korean': REGIONS['All/Total'],
        'region_index': 0,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'Bond'
    },
    {
        'name': 'RawDataDomesticBond',
        'fund_type_korean': FUND_TYPES['Bond'],
        'fund_type_index': 4,
        'region_korean': REGIONS['Domestic'],
        'region_index': 1,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'DomesticBond'
    },
    {
        'name': 'RawDataHybridAsset',
        'fund_type_korean': FUND_TYPES['Hybrid Asset'],
        'fund_type_index': 13, # This is a guess for the "last category in dropdown (14th option)"
        'region_korean': REGIONS['All/Total'],
        'region_index': 0,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'HybridAsset'
    },
    {
        'name': 'RawDataDomesticHybridAsset',
        'fund_type_korean': FUND_TYPES['Hybrid Asset'],
        'fund_type_index': 13,
        'region_korean': REGIONS['Domestic'],
        'region_index': 1,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'DomesticHybridAsset'
    },
    {
        'name': 'RawDataMoneyMarket',
        'fund_type_korean': FUND_TYPES['Money Market'],
        'fund_type_index': 5, # 6th option, so index 5
        'region_korean': REGIONS['All/Total'],
        'region_index': 0,
        'time_window_years': 5,
        'fund_universe_index': 0,
        'output_name': 'MoneyMarket'
    }
]

# --- Column Mapping ---
KOREAN_TO_ENGLISH_CATEGORY = {
    '자산총액': 'Total assets',
    '주식': 'Stock',
    '채권': 'Bonds',
    'CP': 'CP',
    '예금': 'Deposit',
    '콜론': 'Call loan',
    '기타': 'Others'
}

KOREAN_TO_ENGLISH_METRIC = {
    '금액': 'Amount',
    '비중': 'Weight'
}

# --- Placeholder for Time Series Codes (will be generated dynamically) ---
TIMESERIES_CODES = {}
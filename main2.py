"""
Stealth Selenium Script for KOFIA Website
This script uses undetected-chromedriver + selenium-stealth to navigate the KOFIA website
and locate specific menu elements without being detected.
"""

import undetected_chromedriver as uc
from selenium_stealth import stealth
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_stealth_driver():
    """
    Create an undetected Chrome driver with stealth options
    """
    options = uc.ChromeOptions()
    
    # Additional stealth options
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    
    # Optional: Run in headless mode (comment out if you want to see the browser)
    # options.add_argument('--headless=new')
    
    # Set a realistic window size
    options.add_argument('--window-size=1920,1080')
    
    # Create driver
    # Use version_main=139 to match your installed Chrome version (139.0.7258.138)
    driver = uc.Chrome(options=options, version_main=139, use_subprocess=True)

    # Apply selenium-stealth to make detection harder
    logger.info("Applying selenium-stealth configuration...")
    stealth(driver,
        languages=["en-US", "en", "ko-KR", "ko"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    # Additional stealth JavaScript
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": driver.execute_script("return navigator.userAgent").replace('Headless', '')
    })

    # Override navigator properties
    driver.execute_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });

        // Override the plugins to make it look more real
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });

        // Make Chrome object look real
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
        };

        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
    """)

    # Set page load timeout
    driver.set_page_load_timeout(30)

    logger.info("Stealth configuration applied successfully!")

    return driver


def find_fund_menu_element(driver):
    """
    Find and log the specific fund menu element
    """
    try:
        logger.info("Waiting for page to load completely...")
        
        # Wait for the page to be ready
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        
        # Additional wait for dynamic content
        time.sleep(2)
        
        # Find the menu element by class name
        logger.info("Looking for the fund menu element...")
        
        # Try to find the li element with class "menu2"
        menu_elements = driver.find_elements(By.CSS_SELECTOR, "li.menu2")
        
        logger.info(f"Found {len(menu_elements)} elements with class 'menu2'")
        
        # Iterate through menu2 elements to find the one containing the fund link
        target_element = None
        for element in menu_elements:
            try:
                # Check if this element contains the fund link
                fund_link = element.find_element(By.CSS_SELECTOR, "a[href*='MSIS40100000000000']")
                if fund_link:
                    target_element = element
                    logger.info("✓ Found the target fund menu element!")
                    break
            except:
                continue
        
        if target_element:
            # Log element details
            logger.info("\n" + "="*60)
            logger.info("ELEMENT FOUND - Details:")
            logger.info("="*60)
            logger.info(f"Tag: {target_element.tag_name}")
            logger.info(f"Class: {target_element.get_attribute('class')}")
            logger.info(f"HTML:\n{target_element.get_attribute('outerHTML')[:500]}...")
            logger.info("="*60)
            
            # Find and log sub-menu items
            try:
                submenu = target_element.find_element(By.CLASS_NAME, "TopSubMenu")
                submenu_items = submenu.find_elements(By.TAG_NAME, "li")
                logger.info(f"\nFound {len(submenu_items)} sub-menu items:")
                for idx, item in enumerate(submenu_items, 1):
                    link = item.find_element(By.TAG_NAME, "a")
                    img = link.find_element(By.TAG_NAME, "img")
                    alt_text = img.get_attribute("alt")
                    href = link.get_attribute("href")
                    logger.info(f"  {idx}. {alt_text} - {href}")
            except Exception as e:
                logger.warning(f"Could not parse sub-menu: {e}")
            
            return target_element
        else:
            logger.error("Could not find the target fund menu element")
            return None
            
    except Exception as e:
        logger.error(f"Error finding element: {e}")
        logger.error(f"Current URL: {driver.current_url}")
        return None


def main():
    """
    Main function to run the stealth scraper
    """
    driver = None
    
    try:
        logger.info("Initializing stealth Chrome driver...")
        driver = create_stealth_driver()
        
        logger.info("Navigating to KOFIA website...")
        url = "http://freesis.kofia.or.kr/"
        driver.get(url)
        
        logger.info(f"Successfully loaded: {url}")
        logger.info(f"Page title: {driver.title}")

        # IMPORTANT: The website uses FRAMES - we need to switch to the 'main' frame
        logger.info("Switching to 'main' frame...")
        try:
            driver.switch_to.frame("main")
            logger.info("✓ Successfully switched to main frame")

            # Wait for frame content to load
            time.sleep(2)

        except Exception as e:
            logger.error(f"Failed to switch to main frame: {e}")
            raise

        # Take screenshot for debugging (after switching to frame)
        screenshot_path = "kofia_screenshot.png"
        driver.save_screenshot(screenshot_path)
        logger.info(f"Screenshot saved to: {screenshot_path}")

        # Save page source for inspection
        with open("kofia_page_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        logger.info("Page source saved to: kofia_page_source.html")

        # Find the target element
        element = find_fund_menu_element(driver)
        
        if element:
            logger.info("\n✓ Script completed successfully!")
        else:
            logger.warning("\n⚠ Script completed but element was not found")
        
        # Keep browser open for inspection
        logger.info("\nBrowser will remain open for 10 seconds for inspection...")
        time.sleep(10)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        
    finally:
        if driver:
            logger.info("Closing browser...")
            driver.quit()
            logger.info("Browser closed.")


if __name__ == "__main__":
    main()
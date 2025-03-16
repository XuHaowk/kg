#!/usr/bin/env python3
"""
CNKI Selenium Crawler

This script uses Selenium to automate browser interactions with the CNKI (China National Knowledge
Infrastructure) website for searching and downloading academic literature. It's designed to be more
reliable than direct HTTP requests since it automates a real browser, bypassing many anti-scraping
measures.
"""

import os
import sys
import time
import json
import logging
import random
import pandas as pd
import urllib.parse
from datetime import datetime
import re
from pathlib import Path
from bs4 import BeautifulSoup

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException
from webdriver_manager.chrome import ChromeDriverManager


class CNKISeleniumCrawler:
    """CNKI Selenium-based crawler for reliable literature search and download"""
    
    def __init__(self, username="", password="", output_dir="output", headless=False, 
                 download_dir=None, chrome_path=None, debug_mode=True):
        """
        Initialize the CNKI Selenium crawler
        
        Args:
            username (str): CNKI account username (optional)
            password (str): CNKI account password (optional)
            output_dir (str): Directory to store output files and logs
            headless (bool): Whether to run the browser in headless mode
            download_dir (str): Directory for downloaded files (defaults to output_dir/downloads)
            chrome_path (str): Path to Chrome executable (optional)
            debug_mode (bool): Whether to enable extensive debugging output
        """
        self.username = username
        self.password = password
        self.output_dir = os.path.abspath(output_dir)
        self.headless = headless
        self.debug_mode = debug_mode
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create a debug directory for screenshots and page source
        self.debug_dir = os.path.join(self.output_dir, "debug")
        if self.debug_mode:
            os.makedirs(self.debug_dir, exist_ok=True)
        
        # Set up download directory
        if download_dir:
            self.download_dir = os.path.abspath(download_dir)
        else:
            self.download_dir = os.path.join(self.output_dir, "downloads")
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Set up logging
        self.logger = self._setup_logger()
        
        # Initialize browser
        self.driver = self._setup_browser(chrome_path)
        self.wait = WebDriverWait(self.driver, 20)  # 20 second timeout for wait conditions
        
        # Track login status
        self.is_logged_in = False
    
    def _setup_logger(self):
        """Set up the logger for the crawler"""
        logger = logging.getLogger("CNKISeleniumCrawler")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create file handler
        log_path = os.path.join(self.output_dir, f"cnki_selenium_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # Add handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def _setup_browser(self, chrome_path=None):
        """
        Set up Chrome browser with Selenium
        
        Args:
            chrome_path (str): Path to Chrome executable (optional)
            
        Returns:
            webdriver.Chrome: Configured Chrome webdriver
        """
        self.logger.info("Setting up Chrome browser...")
        
        chrome_options = Options()
        
        # Configure headless mode if requested
        if self.headless:
            chrome_options.add_argument("--headless=new")  # For newer Chrome versions
        
        # Configure user agent to mimic a regular browser
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0")
        
        # Additional options to improve reliability
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Disable ChromeDriver automation flag
        chrome_options.add_argument("--disable-extensions")  # Disable extensions
        chrome_options.add_argument("--disable-popup-blocking")  # Allow popups
        chrome_options.add_argument("--disable-infobars")  # Disable info bars
        chrome_options.add_argument("--no-sandbox")  # Bypass OS security model
        chrome_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        chrome_options.add_argument("--disable-gpu")  # Applicable to Windows only
        
        # Add unsafe-swiftshader flag to address WebGL warnings
        chrome_options.add_argument("--enable-unsafe-swiftshader")
        
        # Configure download behavior
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Exclude the "enable-automation" switch
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Set up Chrome driver
        try:
            if chrome_path:
                service = Service(executable_path=chrome_path)
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                # Use webdriver-manager to automatically download the correct driver
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set window size
            driver.set_window_size(1366, 768)
            
            # Execute Chrome DevTools Protocol commands to make detection more difficult
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """
            })
            
            self.logger.info("Chrome browser set up successfully.")
            return driver
            
        except Exception as e:
            self.logger.error(f"Failed to set up Chrome browser: {str(e)}")
            raise
    
    def login(self):
        """
        Log in to CNKI website with enhanced page detection
        
        Returns:
            bool: True if login is successful, False otherwise
        """
        if not self.username or not self.password:
            self.logger.warning("No credentials provided, proceeding in guest mode")
            return False
            
        try:
            self.logger.info(f"Attempting to login to CNKI with username: {self.username}")
            
            # First try direct navigation to login page
            self.driver.get("https://login.cnki.net/")
            time.sleep(3)
            
            # Take screenshot to debug login page structure
            if self.debug_mode:
                self._inspect_page_for_debugging("login_page")
            
            # Look for username field with multiple potential IDs and attributes
            username_field = None
            username_selectors = [
                "//input[@id='username']",
                "//input[@name='username']",
                "//input[@id='TextBoxUserName']",
                "//input[@id='userName']",
                "//input[@placeholder='用户名/手机号/邮箱']",
                "//input[contains(@class, 'username')]"
            ]
            
            for selector in username_selectors:
                try:
                    username_field = self.driver.find_element(By.XPATH, selector)
                    if username_field:
                        break
                except:
                    continue
                    
            if not username_field:
                # If still not found, try to look at all input fields
                self.logger.info("Standard username field not found, looking for any input field")
                all_inputs = self.driver.find_elements(By.TAG_NAME, "input")
                
                # Log what input fields were found for debugging
                for i, input_field in enumerate(all_inputs):
                    field_type = input_field.get_attribute("type")
                    field_id = input_field.get_attribute("id")
                    field_name = input_field.get_attribute("name")
                    self.logger.info(f"Input field {i}: type={field_type}, id={field_id}, name={field_name}")
                    
                    # First text/email input is likely username
                    if field_type in ["text", "email"] and not username_field:
                        username_field = input_field
            
            if not username_field:
                self.logger.error("Could not find any usable username field")
                return False
                
            # Similar approach for password field
            password_field = None
            password_selectors = [
                "//input[@id='password']",
                "//input[@name='password']",
                "//input[@type='password']",
                "//input[contains(@class, 'password')]"
            ]
            
            for selector in password_selectors:
                try:
                    password_field = self.driver.find_element(By.XPATH, selector)
                    if password_field:
                        break
                except:
                    continue
                    
            if not password_field:
                self.logger.error("Could not find password field")
                return False
                
            # Fill credentials
            username_field.clear()
            self._type_slowly(username_field, self.username)
            password_field.clear()
            self._type_slowly(password_field, self.password)
            
            # Find and click login button
            login_button = None
            button_selectors = [
                "//button[@type='submit']",
                "//input[@type='submit']",
                "//button[contains(text(), '登录')]",
                "//input[@value='登录']",
                "//a[contains(text(), '登录')]"
            ]
            
            for selector in button_selectors:
                try:
                    login_button = self.driver.find_element(By.XPATH, selector)
                    if login_button:
                        break
                except:
                    continue
                    
            if not login_button:
                self.logger.error("Could not find login button")
                return False
                
            # Click the login button
            self._click_with_retry(login_button)
            time.sleep(3)
            
            # Take screenshot after login attempt
            if self.debug_mode:
                self._inspect_page_for_debugging("after_login")
            
            # Verify login success by checking for redirects or welcome elements
            if "login.cnki.net" not in self.driver.current_url:
                self.logger.info("Login appears successful - redirected away from login page")
                self.is_logged_in = True
                return True
                
            # Check for visible username or welcome elements
            welcome_selectors = [
                "//a[contains(@href, 'my.cnki.net')]",
                "//a[contains(text(), '我的CNKI')]",
                "//span[contains(@class, 'username')]",
                "//span[contains(text(), '欢迎')]"
            ]
            
            for selector in welcome_selectors:
                try:
                    welcome_element = self.driver.find_element(By.XPATH, selector)
                    if welcome_element and welcome_element.is_displayed():
                        self.logger.info(f"Login confirmed - found welcome element: {welcome_element.text}")
                        self.is_logged_in = True
                        return True
                except:
                    continue
            
            self.logger.error("Login verification failed")
            return False
            
        except Exception as e:
            self.logger.error(f"Login process error: {str(e)}")
            return False
    
    def search_and_collect(self, term, date_range=None, max_results=100, db_code="CJFD"):
        """
        Search CNKI for literature and collect results with enhanced element detection
        
        Args:
            term (str): Search term
            date_range (tuple): Date range as (start_date, end_date) in "YYYY/MM/DD" format
            max_results (int): Maximum number of results to collect
            db_code (str): Database code (CJFD, CDFD, CMFD)
            
        Returns:
            dict: Dictionary containing search results
        """
        try:
            # Convert db_code to human-readable form for logging
            db_name_map = {
                "CJFD": "中国学术期刊",
                "CDFD": "博士论文",
                "CMFD": "硕士论文"
            }
            db_name = db_name_map.get(db_code, db_code)
            
            self.logger.info(f"Searching for term '{term}' in {db_name}")
            
            # Try to log in if credentials are provided
            if self.username and self.password and not self.is_logged_in:
                self.login()
            
            # First try to use the home page search
            self.logger.info("Attempting homepage search")
            home_search_successful = self._try_homepage_search(term)
            
            # If homepage search failed, try advanced search
            if not home_search_successful:
                self.logger.info("Homepage search failed, trying advanced search")
                adv_search_successful = self._try_advanced_search(term, db_code)
                
                # If advanced search also failed, try direct URL search
                if not adv_search_successful:
                    self.logger.info("Advanced search failed, trying direct URL search")
                    direct_search_successful = self._try_direct_url_search(term, db_code)
                    
                    if not direct_search_successful:
                        self.logger.error("All search methods failed")
                        return {"status": "error", "message": "Could not perform search", "results": []}
                    
            # Wait for search results to load
            time.sleep(3)
            
            # Inspect search results page
            if self.debug_mode:
                self._inspect_page_for_debugging("search_results")
            
            # Check for no results
            if self._is_no_results_page():
                self.logger.warning("No search results found")
                return {"status": "success", "count": 0, "results": []}
            
            # Get total result count
            total_count = self._get_result_count()
            if total_count == 0:
                self.logger.warning("No search results found (count is 0)")
                return {"status": "success", "count": 0, "results": []}
                
            self.logger.info(f"Found {total_count} results, will collect up to {max_results}")
            
            # Collect results
            results = self._collect_search_results(max_results)
            
            # Save results
            if results:
                # Create DataFrame
                df = pd.DataFrame(results)
                
                # Save as CSV and JSON
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(self.output_dir, f"cnki_results_{db_code}_{timestamp}.csv")
                json_path = os.path.join(self.output_dir, f"cnki_results_{db_code}_{timestamp}.json")
                
                df.to_csv(csv_path, index=False, encoding='utf-8')
                df.to_json(json_path, orient='records', force_ascii=False, indent=2)
                
                self.logger.info(f"Saved {len(results)} results to {csv_path} and {json_path}")
                
                return {
                    "status": "success", 
                    "count": total_count, 
                    "results": results,
                    "csv_path": csv_path,
                    "json_path": json_path
                }
            else:
                self.logger.warning("No results collected")
                return {"status": "warning", "count": total_count, "results": []}
            
        except Exception as e:
            self.logger.error(f"Search failed: {str(e)}")
            if self.debug_mode:
                self._inspect_page_for_debugging("search_error")
            return {"status": "error", "message": str(e), "results": []}
    
    def _try_homepage_search(self, term):
        """
        Try to search using the homepage search box
        
        Args:
            term (str): Search term
            
        Returns:
            bool: True if search was successful, False otherwise
        """
        try:
            # Navigate to homepage
            self.driver.get("https://www.cnki.net/")
            time.sleep(3)
            
            if self.debug_mode:
                self._inspect_page_for_debugging("homepage")
            
            # Try to find the search input
            search_input = None
            search_selectors = [
                "//input[@id='txt_search']",
                "//input[contains(@placeholder, '搜索')]",
                "//input[contains(@class, 'search-input')]",
                "//input[contains(@class, 'input-box')]"
            ]
            
            for selector in search_selectors:
                try:
                    search_input = self.driver.find_element(By.XPATH, selector)
                    if search_input and search_input.is_displayed():
                        break
                except:
                    continue
            
            if not search_input:
                self.logger.warning("Could not find homepage search input")
                return False
                
            # Enter search term
            search_input.clear()
            self._type_slowly(search_input, term)
            
            # Find search button
            search_button = None
            button_selectors = [
                "//input[@type='submit']",
                "//button[contains(@class, 'search-btn')]",
                "//img[contains(@class, 'search-btn')]",
                "//div[contains(@class, 'search-btn')]"
            ]
            
            for selector in button_selectors:
                try:
                    search_button = self.driver.find_element(By.XPATH, selector)
                    if search_button and search_button.is_displayed():
                        break
                except:
                    continue
            
            if not search_button:
                # Try sending Enter key instead
                self.logger.info("Search button not found, trying Enter key")
                search_input.send_keys(Keys.RETURN)
            else:
                # Click search button
                self._click_with_retry(search_button)
                
            # Wait for results page to load
            time.sleep(3)
            
            # Check if we're on a results page
            results_page_indicators = [
                "kns8/defaultresult", 
                "search_result",
                "brief/result.aspx"
            ]
            
            for indicator in results_page_indicators:
                if indicator in self.driver.current_url:
                    self.logger.info(f"Homepage search successful - redirected to {self.driver.current_url}")
                    return True
            
            self.logger.warning(f"Homepage search may have failed - current URL: {self.driver.current_url}")
            return False
            
        except Exception as e:
            self.logger.error(f"Homepage search error: {str(e)}")
            return False
    
    def _try_advanced_search(self, term, db_code="CJFD"):
        """
        Try to search using the advanced search page
        
        Args:
            term (str): Search term
            db_code (str): Database code
            
        Returns:
            bool: True if search was successful, False otherwise
        """
        try:
            # Navigate to advanced search page
            self.driver.get("https://kns.cnki.net/kns8/AdvSearch")
            time.sleep(3)
            
            if self.debug_mode:
                self._inspect_page_for_debugging("advanced_search")
            
            # Try to select database
            try:
                # Find and click the database selection dropdown
                db_selectors = [
                    "//div[contains(@class, 'sort-list')]",
                    "//div[contains(@class, 'database-list')]",
                    "//div[contains(@class, 'database-select')]"
                ]
                
                db_dropdown = None
                for selector in db_selectors:
                    try:
                        db_dropdown = self.driver.find_element(By.XPATH, selector)
                        if db_dropdown and db_dropdown.is_displayed():
                            break
                    except:
                        continue
                
                if db_dropdown:
                    self._click_with_retry(db_dropdown)
                    time.sleep(1)
                    
                    # Find and click the specific database option
                    db_option_selectors = [
                        f"//a[@data-value='{db_code}']",
                        f"//a[contains(text(), '{db_code}')]",
                        f"//div[contains(@class, 'db-option')][contains(text(), '{db_code}')]"
                    ]
                    
                    db_option = None
                    for selector in db_option_selectors:
                        try:
                            db_option = self.driver.find_element(By.XPATH, selector)
                            if db_option and db_option.is_displayed():
                                break
                        except:
                            continue
                    
                    if db_option:
                        self._click_with_retry(db_option)
                        time.sleep(1)
                    else:
                        self.logger.warning(f"Could not find database option for {db_code}")
                else:
                    self.logger.warning("Could not find database dropdown")
                    
            except Exception as e:
                self.logger.warning(f"Could not select database: {str(e)}. Will use default database.")
            
            # Try to find search input field
            search_input = None
            search_selectors = [
                "//input[@id='advSearchKeywords']",
                "//textarea[@id='advSearchKeywords']",
                "//input[contains(@class, 'search-input')]",
                "//textarea[contains(@class, 'search-input')]",
                "//input[contains(@placeholder, '检索词')]",
                "//textarea[contains(@placeholder, '检索词')]"
            ]
            
            for selector in search_selectors:
                try:
                    search_input = self.driver.find_element(By.XPATH, selector)
                    if search_input and search_input.is_displayed():
                        break
                except:
                    continue
            
            if not search_input:
                self.logger.error("Could not find search input field")
                return False
                
            # Enter search term
            search_input.clear()
            self._type_slowly(search_input, term)
            
            # Find search button
            search_button = None
            button_selectors = [
                "//button[contains(@class, 'search-btn')]",
                "//input[@type='submit']",
                "//button[contains(text(), '检索')]",
                "//div[contains(@class, 'search-btn')]"
            ]
            
            for selector in button_selectors:
                try:
                    search_button = self.driver.find_element(By.XPATH, selector)
                    if search_button and search_button.is_displayed():
                        break
                except:
                    continue
            
            if not search_button:
                self.logger.error("Could not find search button")
                return False
                
            # Click search button
            self._click_with_retry(search_button)
            
            # Wait for results to load
            time.sleep(3)
            
            # Check if we're on a results page
            results_page_indicators = [
                "kns8/defaultresult", 
                "search_result",
                "brief/result.aspx"
            ]
            
            for indicator in results_page_indicators:
                if indicator in self.driver.current_url:
                    self.logger.info(f"Advanced search successful - redirected to {self.driver.current_url}")
                    return True
            
            self.logger.warning(f"Advanced search may have failed - current URL: {self.driver.current_url}")
            return False
            
        except Exception as e:
            self.logger.error(f"Advanced search error: {str(e)}")
            return False
    
    def _try_direct_url_search(self, term, db_code="CJFD"):
        """
        Try to search using direct URL construction
        
        Args:
            term (str): Search term
            db_code (str): Database code
            
        Returns:
            bool: True if search was successful, False otherwise
        """
        try:
            # Encode term for URL
            encoded_term = urllib.parse.quote(term)
            
            # Try different URL patterns (CNKI has changed its URL structure over time)
            url_patterns = [
                f"https://kns.cnki.net/kns8/defaultresult/index?kw={encoded_term}&korder=SU&dbcode={db_code}",
                f"https://kns.cnki.net/kns/brief/result.aspx?dbprefix={db_code}&kw={encoded_term}",
                f"https://kns.cnki.net/kns8/defaultresult/index?kw={encoded_term}&korder=SU&dbcode={db_code}&searchType=0"
            ]
            
            for url in url_patterns:
                self.logger.info(f"Trying direct URL: {url}")
                self.driver.get(url)
                time.sleep(3)
                
                if self.debug_mode:
                    self._inspect_page_for_debugging(f"direct_url_{url_patterns.index(url)}")
                
                # Check if we're on a results page
                results_page_indicators = [
                    "kns8/defaultresult", 
                    "search_result",
                    "brief/result.aspx"
                ]
                
                for indicator in results_page_indicators:
                    if indicator in self.driver.current_url:
                        self.logger.info(f"Direct URL search successful - on {self.driver.current_url}")
                        return True
            
            self.logger.warning("All direct URL patterns failed")
            return False
            
        except Exception as e:
            self.logger.error(f"Direct URL search error: {str(e)}")
            return False
    
    def _is_no_results_page(self):
        """
        Check if the current page is a "no results" page
        
        Returns:
            bool: True if no results were found, False otherwise
        """
        try:
            # Look for common "no results" indicators
            no_results_selectors = [
                "//div[contains(text(), '抱歉')]",
                "//div[contains(text(), '没有检索到相关结果')]",
                "//div[contains(@class, 'no-result')]",
                "//div[contains(text(), '没有找到')]"
            ]
            
            for selector in no_results_selectors:
                try:
                    element = self.driver.find_element(By.XPATH, selector)
                    if element and element.is_displayed():
                        return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Error checking for no results: {str(e)}")
            return False
    
    def _get_result_count(self):
        """
        Get the total count of search results
        
        Returns:
            int: Total number of results, or 0 if count cannot be determined
        """
        try:
            # Look for result count elements with different selectors
            count_selectors = [
                "//div[contains(@class, 'search-count')]",
                "//span[contains(@class, 'total-text')]",
                "//div[contains(@class, 'pager')]"
            ]
            
            for selector in count_selectors:
                try:
                    count_element = self.driver.find_element(By.XPATH, selector)
                    if count_element:
                        count_text = count_element.text
                        # Look for patterns like "共xx条结果", "Found xx results", etc.
                        count_match = re.search(r'共\s*(\d+(?:,\d+)*)\s*条', count_text)
                        if not count_match:
                            count_match = re.search(r'(\d+(?:,\d+)*)\s*条结果', count_text)
                        if not count_match:
                            count_match = re.search(r'Found\s*(\d+(?:,\d+)*)\s*results', count_text)
                        if not count_match:
                            count_match = re.search(r'(\d+(?:,\d+)*)\s*results', count_text)
                        if not count_match:
                            # Generic digit extraction as last resort
                            count_match = re.search(r'(\d+(?:,\d+)*)', count_text)
                            
                        if count_match:
                            # Remove commas from number
                            count_str = count_match.group(1).replace(',', '')
                            return int(count_str)
                except:
                    continue
            
            # If we can't find a count element, try counting result items directly
            result_items = self._find_result_items()
            if result_items:
                return len(result_items)
            
            # If all else fails
            return 0
            
        except Exception as e:
            self.logger.warning(f"Error getting result count: {str(e)}")
            return 0
    
    def _find_result_items(self):
        """
        Find result items on the current page with multiple selectors
        
        Returns:
            list: List of WebElement objects representing result items
        """
        # Try different selectors for result items
        result_selectors = [
            "//tr[contains(@class, 'result-table-tr')]",
            "//div[contains(@class, 'result-item')]",
            "//div[contains(@class, 'list-item')]",
            "//div[contains(@class, 'search-result')]/div",
            "//table[@id='gridTable']/tbody/tr[position()>1]"  # Skip header row
        ]
        
        for selector in result_selectors:
            try:
                items = self.driver.find_elements(By.XPATH, selector)
                if items and len(items) > 0:
                    self.logger.info(f"Found {len(items)} result items using selector: {selector}")
                    return items
            except:
                continue
        
        return []
    
    def _collect_search_results(self, max_results):
        """
        Collect search results from all pages until max_results is reached
        
        Args:
            max_results (int): Maximum number of results to collect
            
        Returns:
            list: List of result dictionaries
        """
        results = []
        current_page = 1
        
        while len(results) < max_results:
            self.logger.info(f"Processing page {current_page}")
            
            # Get result items on current page
            items = self._find_result_items()
            
            if not items:
                self.logger.warning(f"No items found on page {current_page}")
                break
                
            # Process each item
            for item in items:
                if len(results) >= max_results:
                    break
                    
                try:
                    result = self._extract_result_data(item)
                    if result:
                        results.append(result)
                except Exception as e:
                    self.logger.warning(f"Error extracting data from item: {str(e)}")
            
            # Check if we need to go to next page
            if len(results) < max_results:
                if not self._go_to_next_page():
                    self.logger.info("No more pages available")
                    break
                current_page += 1
                time.sleep(2)
        
        self.logger.info(f"Collected {len(results)} results from {current_page} pages")
        return results
    
    def _extract_result_data(self, item):
        """
        Extract data from a single result item
        
        Args:
            item: WebElement representing a result item
            
        Returns:
            dict: Extracted data or None if extraction failed
        """
        try:
            # Take screenshot of the item for debugging
            if self.debug_mode:
                try:
                    item.screenshot(os.path.join(self.debug_dir, f"result_item_{int(time.time())}.png"))
                except:
                    pass
            
            # Try to find title element with multiple possible selectors
            title_element = None
            title_selectors = [
                ".//a[contains(@class, 'title')]",
                ".//a[contains(@class, 'name')]",
                ".//a[contains(@class, 'fz14')]",
                ".//a[contains(@onclick, 'openDetail')]",
                ".//a[not(@class)]",  # Sometimes CNKI uses plain anchors
                ".//div[contains(@class, 'title')]/a"
            ]
            
            for selector in title_selectors:
                try:
                    title_element = item.find_element(By.XPATH, selector)
                    if title_element and title_element.text.strip():
                        break
                except:
                    continue
            
            if not title_element:
                self.logger.warning("Could not find title element, skipping item")
                return None
                
            title = title_element.text.strip()
            link = title_element.get_attribute("href") or ""
            
            # Try to find author with multiple selectors
            authors = ""
            author_selectors = [
                ".//td[contains(@class, 'author')]",
                ".//div[contains(@class, 'author')]",
                ".//span[contains(@class, 'author')]",
                ".//p[contains(@class, 'author')]"
            ]
            
            for selector in author_selectors:
                try:
                    author_element = item.find_element(By.XPATH, selector)
                    if author_element:
                        authors = author_element.text.strip()
                        break
                except:
                    continue
            
            # Try to find source with multiple selectors
            source = ""
            source_selectors = [
                ".//td[contains(@class, 'source')]",
                ".//div[contains(@class, 'source')]",
                ".//span[contains(@class, 'source')]",
                ".//a[contains(@class, 'source')]"
            ]
            
            for selector in source_selectors:
                try:
                    source_element = item.find_element(By.XPATH, selector)
                    if source_element:
                        source = source_element.text.strip()
                        break
                except:
                    continue
            
            # Try to find date with multiple selectors
            pub_date = ""
            date_selectors = [
                ".//td[contains(@class, 'date')]",
                ".//div[contains(@class, 'date')]",
                ".//span[contains(@class, 'date')]",
                ".//td[position()=last()]"  # Often date is in the last column
            ]
            
            for selector in date_selectors:
                try:
                    date_element = item.find_element(By.XPATH, selector)
                    if date_element:
                        pub_date = date_element.text.strip()
                        break
                except:
                    continue
                    
            # Return the collected data
            return {
                "title": title,
                "authors": authors,
                "source": source,
                "publication_date": pub_date,
                "link": link,
                "database": "CNKI"
            }
            
        except Exception as e:
            self.logger.warning(f"Error extracting result data: {str(e)}")
            return None
    
    def _go_to_next_page(self):
        """
        Navigate to the next page of results
        
        Returns:
            bool: True if successfully navigated to next page, False otherwise
        """
        try:
            # Try to find next page button with multiple selectors
            next_button = None
            next_selectors = [
                "//a[@id='PageNext']",
                "//a[contains(@class, 'next')]",
                "//a[contains(text(), '下一页')]",
                "//a[contains(@href, 'page=')][@class='next']"
            ]
            
            for selector in next_selectors:
                try:
                    next_button = self.driver.find_element(By.XPATH, selector)
                    if next_button and next_button.is_displayed():
                        # Check if button is disabled
                        if "disabled" in next_button.get_attribute("class") or "last" in next_button.get_attribute("class"):
                            self.logger.info("Next button is disabled (last page)")
                            return False
                        break
                except:
                    continue
            
            if not next_button:
                self.logger.info("Could not find next page button")
                return False
                
            # Click the next button
            self._click_with_retry(next_button)
            
            # Wait for page to load
            time.sleep(3)
            
            return True
            
        except Exception as e:
            self.logger.warning(f"Error navigating to next page: {str(e)}")
            return False
    
    def manual_collection_mode(self, search_term, output_dir):
        """
        Launch a browser for manual search and provide tools to collect data
        
        This approach gives the user control of the browser but provides
        automated collection of search results.
        
        Args:
            search_term (str): Initial search term to populate
            output_dir (str): Directory to save collected data
            
        Returns:
            dict: Results from the manual collection process
        """
        try:
            self.logger.info("Starting manual collection mode")
            
            # Create a browser instance without automation flags
            # to avoid detection
            chrome_options = Options()
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Open CNKI homepage
            driver.get("https://www.cnki.net/")
            
            # Show instructions to the user
            print("\n" + "="*80)
            print("MANUAL COLLECTION MODE INSTRUCTIONS")
            print("="*80)
            print("1. A browser window has been opened to CNKI")
            print("2. Please login with your credentials manually")
            print("3. Search for your term manually")
            print("4. Navigate to the search results page")
            print("5. When you are viewing the search results, press Enter in this console")
            print("   to automatically collect the data from the current page")
            print("6. You can repeat this process for multiple search result pages")
            print("7. Type 'exit' and press Enter when finished")
            print("="*80 + "\n")
            
            # Wait for user commands
            results_collected = []
            while True:
                command = input("\nPress Enter to collect data from current page, or type 'exit' to finish: ")
                
                if command.lower() == 'exit':
                    break
                    
                try:
                    # Collect data from current page
                    print("Collecting data from current page...")
                    
                    # Check if we're on a search results page
                    if not any(x in driver.current_url for x in ['kns8/defaultresult', 'kns/brief', 'search_result']):
                        print("Warning: Current page doesn't appear to be a search results page.")
                        continue
                    
                    # Get all result items
                    results = []
                    
                    # Try different result item selectors
                    selectors = [
                        "//tr[contains(@class, 'result-table-tr')]",
                        "//div[contains(@class, 'result-item')]",
                        "//div[contains(@class, 'list-item')]"
                    ]
                    
                    items = []
                    for selector in selectors:
                        items = driver.find_elements(By.XPATH, selector)
                        if items:
                            print(f"Found {len(items)} results using selector: {selector}")
                            break
                    
                    if not items:
                        print("No results found on this page. Try another selector.")
                        continue
                    
                    # Process each result
                    for item in items:
                        try:
                            # Extract title and link
                            title_element = item.find_element(By.XPATH, ".//a[contains(@class, 'title') or contains(@class, 'name')]")
                            title = title_element.text.strip()
                            link = title_element.get_attribute("href")
                            
                            # Extract other metadata
                            authors = ""
                            source = ""
                            pub_date = ""
                            
                            try:
                                authors = item.find_element(By.XPATH, ".//td[contains(@class, 'author')] | .//span[contains(@class, 'author')]").text.strip()
                            except:
                                pass
                                
                            try:
                                source = item.find_element(By.XPATH, ".//td[contains(@class, 'source')] | .//span[contains(@class, 'source')]").text.strip()
                            except:
                                pass
                                
                            try:
                                pub_date = item.find_element(By.XPATH, ".//td[contains(@class, 'date')] | .//span[contains(@class, 'date')]").text.strip()
                            except:
                                pass
                            
                            results.append({
                                "title": title,
                                "authors": authors,
                                "source": source,
                                "publication_date": pub_date,
                                "link": link,
                                "database": "CNKI"
                            })
                            
                        except Exception as e:
                            print(f"Error extracting article data: {str(e)}")
                    
                    # Save results
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    page_number = input("Enter page number or identifier for this data: ")
                    
                    # Create DataFrame and save
                    df = pd.DataFrame(results)
                    csv_path = os.path.join(output_dir, f"cnki_manual_page{page_number}_{timestamp}.csv")
                    json_path = os.path.join(output_dir, f"cnki_manual_page{page_number}_{timestamp}.json")
                    
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
                    
                    print(f"Collected {len(results)} articles from this page")
                    print(f"Data saved to {csv_path} and {json_path}")
                    
                    results_collected.extend(results)
                    
                except Exception as e:
                    print(f"Error collecting data: {str(e)}")
            
            # Closing browser
            driver.quit()
            print("Browser closed. Manual collection completed.")
            
            return {
                "status": "success", 
                "message": "Manual collection completed",
                "results": results_collected
            }
            
        except Exception as e:
            self.logger.error(f"Error in manual collection mode: {str(e)}")
            return {"status": "error", "message": str(e), "results": []}
    
    def _inspect_page_for_debugging(self, description="current_page"):
        """
        Inspect the page source and save it for debugging
        
        Args:
            description (str): Description to use in filenames
        """
        if not self.debug_mode:
            return
            
        try:
            # Create a timestamp to ensure unique filenames
            timestamp = int(time.time())
            
            # Take a screenshot
            screenshot_path = os.path.join(self.debug_dir, f"{description}_{timestamp}.png")
            self.driver.save_screenshot(screenshot_path)
            
            # Save the page source
            source_path = os.path.join(self.debug_dir, f"{description}_{timestamp}.html")
            with open(source_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
            
            # Log some basic page info
            self.logger.info(f"Current URL: {self.driver.current_url}")
            
            # List all input fields
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            self.logger.info(f"Found {len(inputs)} input elements on page")
            for i, inp in enumerate(inputs[:10]):  # Limit to first 10 to avoid too much logging
                inp_id = inp.get_attribute("id") or "none"
                inp_name = inp.get_attribute("name") or "none"
                inp_type = inp.get_attribute("type") or "none"
                inp_class = inp.get_attribute("class") or "none"
                self.logger.info(f"Input {i}: id={inp_id}, name={inp_name}, type={inp_type}, class={inp_class}")
            
            # List all buttons
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            self.logger.info(f"Found {len(buttons)} button elements on page")
            for i, btn in enumerate(buttons[:10]):
                btn_text = btn.text or "none"
                btn_id = btn.get_attribute("id") or "none"
                btn_class = btn.get_attribute("class") or "none"
                self.logger.info(f"Button {i}: text='{btn_text}', id={btn_id}, class={btn_class}")
            
            # Log forms and their action attributes
            forms = self.driver.find_elements(By.TAG_NAME, "form")
            self.logger.info(f"Found {len(forms)} forms on page")
            for i, form in enumerate(forms):
                form_id = form.get_attribute("id") or "none"
                form_action = form.get_attribute("action") or "none"
                form_method = form.get_attribute("method") or "none"
                self.logger.info(f"Form {i}: id={form_id}, action={form_action}, method={form_method}")
                
            self.logger.info(f"Debug info saved to {screenshot_path} and {source_path}")
        except Exception as e:
            self.logger.error(f"Error during page inspection: {str(e)}")
    
    def _find_element_with_multiple_selectors(self, selectors, timeout=5):
        """
        Try to find an element using multiple selectors
        
        Args:
            selectors (list): List of XPath selectors to try
            timeout (int): Timeout in seconds for each selector
            
        Returns:
            WebElement or None: Found element or None if not found
        """
        for selector in selectors:
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                return element
            except TimeoutException:
                continue
        return None
    
    def _is_element_present(self, by, value, timeout=2):
        """
        Check if an element is present on the page
        
        Args:
            by: Selenium By strategy
            value: Selector value
            timeout: Timeout in seconds
            
        Returns:
            bool: True if element is present, False otherwise
        """
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return True
        except TimeoutException:
            return False
    
    def _click_with_retry(self, element, max_attempts=3):
        """
        Click an element with retry logic for StaleElementReferenceException
        
        Args:
            element: WebElement to click
            max_attempts: Maximum number of click attempts
            
        Returns:
            bool: True if click succeeded, False otherwise
        """
        for attempt in range(max_attempts):
            try:
                # Try regular click first
                element.click()
                return True
            except ElementNotInteractableException:
                # If regular click fails, try JavaScript click
                try:
                    self.driver.execute_script("arguments[0].click();", element)
                    return True
                except Exception:
                    # If both fail, try scrolling to element and clicking
                    try:
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(0.5)
                        element.click()
                        return True
                    except Exception as e:
                        self.logger.warning(f"Click attempt {attempt+1} failed: {str(e)}")
                        time.sleep(0.5)
            except Exception as e:
                self.logger.warning(f"Click attempt {attempt+1} failed: {str(e)}")
                time.sleep(0.5)
        return False
    
    def _type_slowly(self, element, text, min_delay=0.05, max_delay=0.15):
        """
        Type text into an element with random delays between keystrokes
        
        Args:
            element: WebElement to type into
            text: Text to type
            min_delay: Minimum delay between keystrokes
            max_delay: Maximum delay between keystrokes
        """
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(min_delay, max_delay))
    
    def wait_with_random_delay(self, min_seconds, max_seconds):
        """
        Wait for a random amount of time within the specified range
        
        Args:
            min_seconds: Minimum wait time in seconds
            max_seconds: Maximum wait time in seconds
        """
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def close(self):
        """Close the browser and clean up"""
        try:
            if self.driver:
                self.driver.quit()
                self.logger.info("Browser closed successfully.")
        except Exception as e:
            self.logger.error(f"Error closing browser: {str(e)}")


def main():
    """Command-line interface for the CNKI Selenium crawler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CNKI Selenium Crawler')
    parser.add_argument('--username', '-u', default='', help='CNKI username')
    parser.add_argument('--password', '-p', default='', help='CNKI password')
    parser.add_argument('--term', '-t', required=True, help='Search term')
    parser.add_argument('--start-date', '-s', default='2010/01/01', help='Start date (YYYY/MM/DD)')
    parser.add_argument('--end-date', '-e', default='', help='End date (YYYY/MM/DD), defaults to current date')
    parser.add_argument('--max-results', '-m', type=int, default=100, help='Maximum number of results to collect')
    parser.add_argument('--db-code', '-d', default='CJFD', choices=['CJFD', 'CDFD', 'CMFD'], 
                        help='Database code: CJFD (journals), CDFD (PhD theses), CMFD (Master theses)')
    parser.add_argument('--output-dir', '-o', default='output', help='Output directory')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode (no browser window)')
    parser.add_argument('--manual', action='store_true', help='Run in manual collection mode')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode (extra logging and screenshots)')
    parser.add_argument('--chrome-path', default='', help='Path to Chrome executable')
    
    args = parser.parse_args()
    
    # Set default end date if not provided
    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y/%m/%d')
    
    # Create crawler
    crawler = CNKISeleniumCrawler(
        username=args.username,
        password=args.password,
        output_dir=args.output_dir,
        headless=args.headless,
        chrome_path=args.chrome_path,
        debug_mode=args.debug
    )
    
    try:
        if args.manual:
            # Run in manual mode
            crawler.manual_collection_mode(args.term, args.output_dir)
        else:
            # Run in automatic mode
            search_results = crawler.search_and_collect(
                term=args.term,
                date_range=(args.start_date, args.end_date),
                max_results=args.max_results,
                db_code=args.db_code
            )
            
            if search_results["status"] == "success":
                print(f"Search completed successfully.")
                print(f"Found {search_results.get('count', 0)} total results, collected {len(search_results.get('results', []))}.")
                
                if search_results.get('csv_path'):
                    print(f"Results saved to: {search_results['csv_path']}")
            else:
                print(f"Search failed: {search_results.get('message', 'Unknown error')}")
        
    finally:
        # Close browser
        crawler.close()


if __name__ == "__main__":
    main()
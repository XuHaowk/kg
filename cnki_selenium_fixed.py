#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CNKI Web Scraper with Adaptive Capabilities
This script provides a robust, flexible scraper for the China National Knowledge Infrastructure (CNKI)
with multiple fallback mechanisms and adaptive page structure detection.
"""

import os
import time
import json
import logging
import urllib.parse
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    ElementNotInteractableException, 
    NoSuchElementException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager


class CNKIWebScraper:
    """
    A robust web scraper for CNKI with adaptive capabilities to handle website changes
    and multiple fallback mechanisms.
    """
    
    def __init__(self, 
                 username=None, 
                 password=None, 
                 output_dir="./cnki_results", 
                 headless=False, 
                 debug_mode=False,
                 chrome_path=None):
        """
        Initialize the CNKI web scraper.
        
        Args:
            username (str, optional): CNKI username for login
            password (str, optional): CNKI password for login
            output_dir (str, optional): Directory to save results
            headless (bool, optional): Run browser in headless mode
            debug_mode (bool, optional): Enable debug mode with screenshots
            chrome_path (str, optional): Path to Chrome binary
        """
        # Setup logging
        self.logger = self._setup_logger()
        self.logger.info("Initializing CNKI Web Scraper")
        
        # Store parameters
        self.username = username
        self.password = password
        self.output_dir = output_dir
        self.headless = headless
        self.debug_mode = debug_mode
        
        # Create output directories
        self._create_directories()
        
        # Setup Chrome browser
        self.driver = self._setup_browser(chrome_path)
        
        # Track login state
        self.is_logged_in = False
        
        self.logger.info("CNKI Web Scraper initialized")
    
    def _setup_logger(self):
        """Set up and configure logger"""
        logger = logging.getLogger("CNKIWebScraper")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
        
        return logger
    
    def _create_directories(self):
        """Create necessary directories for output and debug info"""
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f"Created output directory: {self.output_dir}")
        
        # Create debug directory if debug mode is enabled
        if self.debug_mode:
            self.debug_dir = os.path.join(self.output_dir, "debug")
            if not os.path.exists(self.debug_dir):
                os.makedirs(self.debug_dir)
                self.logger.info(f"Created debug directory: {self.debug_dir}")
    
    def _setup_browser(self, chrome_path=None):
        """Set up Chrome browser with improved SSL handling"""
        self.logger.info("Setting up Chrome browser...")
        
        chrome_options = Options()
        
        # Add options to bypass SSL certificate errors
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument('--allow-insecure-localhost')
        chrome_options.add_argument('--disable-web-security')
        
        # Set headless mode if enabled
        if self.headless:
            chrome_options.add_argument('--headless')
        
        # Set window size
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Disable automation flags to avoid detection
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Set Chrome binary path if provided
        if chrome_path:
            chrome_options.binary_location = chrome_path
        
        # Create and return Chrome driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set default timeout
        driver.implicitly_wait(10)
        
        self.logger.info("Chrome browser set up successfully")
        return driver
    
    def _inspect_page_for_debugging(self, identifier):
        """Capture page information for debugging"""
        if not self.debug_mode:
            return
            
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_base = f"{identifier}_{timestamp}"
            
            # Take screenshot
            screenshot_path = os.path.join(self.debug_dir, f"{filename_base}.png")
            self.driver.save_screenshot(screenshot_path)
            
            # Save page source
            page_source_path = os.path.join(self.debug_dir, f"{filename_base}.html")
            with open(page_source_path, "w", encoding="utf-8") as f:
                f.write(self.driver.page_source)
                
            self.logger.info(f"Saved debug info for {identifier} to {self.debug_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to save debug info: {str(e)}")
    
    def _is_element_present(self, by, value, timeout=5):
        """Check if an element is present on the page"""
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return True
        except TimeoutException:
            return False
    
    def _find_element_with_multiple_selectors(self, selectors, timeout=5):
        """Try multiple selectors to find an element, return the first match"""
        for selector in selectors:
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                return element
            except TimeoutException:
                continue
        return None
    
    def _click_with_retry(self, element, max_retries=3):
        """Try to click an element with multiple methods and retries"""
        methods = [
            lambda e: e.click(),
            lambda e: self.driver.execute_script("arguments[0].click();", e)
        ]
        
        for method_idx, click_method in enumerate(methods):
            for retry in range(max_retries):
                try:
                    click_method(element)
                    time.sleep(1)  # Wait a bit after click
                    return True
                except (ElementNotInteractableException, StaleElementReferenceException) as e:
                    self.logger.warning(f"Click method {method_idx} failed, attempt {retry+1}/{max_retries}: {str(e)}")
                    time.sleep(1)  # Wait before retry
                    
        self.logger.error("All click methods failed")
        return False
    
    def login(self):
        """Login with better iframe and overlay handling"""
        if not self.username or not self.password:
            self.logger.warning("No credentials provided, proceeding in guest mode")
            return False
            
        try:
            self.logger.info(f"Attempting to login to CNKI with username: {self.username}")
            
            # First navigate to the homepage
            self.driver.get("https://www.cnki.net/")
            time.sleep(3)
            
            if self.debug_mode:
                self._inspect_page_for_debugging("cnki_homepage")
                
            # Check for login elements and frames
            frames = self.driver.find_elements(By.TAG_NAME, "iframe")
            self.logger.info(f"Found {len(frames)} iframes on page")
            
            # Look for login button or link
            login_link_selectors = [
                "//a[contains(@href, 'login')]",
                "//a[contains(@onclick, 'login')]",
                "//div[contains(@class, 'login')]",
                "//a[contains(text(), '登录')]"
            ]
            
            login_link = self._find_element_with_multiple_selectors(login_link_selectors)
            if login_link:
                self.logger.info("Found login link, clicking it")
                self._click_with_retry(login_link)
                time.sleep(2)
            
            # Check for login overlay
            overlay_selectors = [
                "//div[contains(@class, 'login-box')]",
                "//div[contains(@class, 'ecp-login')]",
                "//div[contains(@class, 'overlay')]",
                "//div[contains(@class, 'modal')]"
            ]
            
            login_overlay = self._find_element_with_multiple_selectors(overlay_selectors)
            if login_overlay:
                self.logger.info("Found login overlay")
                
                # Try to find username and password fields within the overlay
                username_field = login_overlay.find_element(By.XPATH, ".//input[contains(@class, 'userName') or contains(@class, 'username')]")
                password_field = login_overlay.find_element(By.XPATH, ".//input[contains(@class, 'passWord') or contains(@class, 'password') or @type='password']")
                
                if username_field and password_field:
                    # Try JavaScript to set values directly
                    self.driver.execute_script("arguments[0].value = arguments[1]", username_field, self.username)
                    self.driver.execute_script("arguments[0].value = arguments[1]", password_field, self.password)
                    
                    # Look for login button
                    login_button = login_overlay.find_element(By.XPATH, ".//button[contains(@class, 'login') or contains(@onclick, 'login')]")
                    if login_button:
                        self.driver.execute_script("arguments[0].click()", login_button)
                        time.sleep(3)
                        
                        # Check if login was successful
                        if "login" not in self.driver.current_url:
                            self.is_logged_in = True
                            self.logger.info("Login successful")
                            return True
            
            # If overlay approach failed, try direct navigation to login page
            self.logger.info("Trying direct navigation to login page")
            self.driver.get("https://login.cnki.net/")
            time.sleep(3)
            
            # Try multiple selectors for username and password fields
            username_selectors = [
                "//input[@id='TextBoxUserName']",
                "//input[contains(@id, 'username')]",
                "//input[contains(@class, 'userName')]",
                "//input[@type='text']"
            ]
            
            password_selectors = [
                "//input[@id='TextBoxPassword']",
                "//input[contains(@id, 'password')]",
                "//input[contains(@class, 'passWord')]",
                "//input[@type='password']"
            ]
            
            username_field = self._find_element_with_multiple_selectors(username_selectors)
            password_field = self._find_element_with_multiple_selectors(password_selectors)
            
            if username_field and password_field:
                # Try to use clear() and send_keys() first
                try:
                    username_field.clear()
                    username_field.send_keys(self.username)
                    password_field.clear()
                    password_field.send_keys(self.password)
                except Exception as e:
                    self.logger.warning(f"Standard input method failed: {str(e)}")
                    # Try JavaScript method
                    self.driver.execute_script("arguments[0].value = arguments[1]", username_field, self.username)
                    self.driver.execute_script("arguments[0].value = arguments[1]", password_field, self.password)
                
                # Find login button
                login_button_selectors = [
                    "//input[@type='submit']",
                    "//button[contains(@class, 'login')]",
                    "//button[contains(text(), '登录')]",
                    "//input[contains(@value, '登录')]"
                ]
                
                login_button = self._find_element_with_multiple_selectors(login_button_selectors)
                if login_button:
                    self._click_with_retry(login_button)
                    time.sleep(3)
                    
                    # Check if login was successful
                    if "login" not in self.driver.current_url:
                        self.is_logged_in = True
                        self.logger.info("Login successful")
                        return True
            
            self.logger.warning("Login failed after multiple attempts")
            return False
            
        except Exception as e:
            self.logger.error(f"Login error: {str(e)}")
            if self.debug_mode:
                self._inspect_page_for_debugging("login_error")
            return False
    
    def _try_homepage_search(self, term, db_code="CJFD"):
        """
        Try to search using the homepage search box
        
        Args:
            term: Search term
            db_code: Database code
            
        Returns:
            bool: Success indicator
        """
        try:
            self.logger.info(f"Attempting homepage search for term: {term}")
            
            # Navigate to CNKI homepage
            self.driver.get("https://www.cnki.net/")
            time.sleep(3)
            
            # Wait for search box to be present
            search_box_selectors = [
                "//input[@id='txt_SearchText']",
                "//input[contains(@placeholder, '搜索')]",
                "//input[contains(@class, 'search-input')]",
                "//div[contains(@class, 'search-box')]//input",
                "//div[contains(@class, 'input-box')]//input"
            ]
            
            search_box = self._find_element_with_multiple_selectors(search_box_selectors)
            if not search_box:
                self.logger.warning("Could not find search box on homepage")
                return False
            
            # Clear and enter search term
            search_box.clear()
            search_box.send_keys(term)
            
            # Find and click search button
            search_button_selectors = [
                "//button[contains(@class, 'search-btn')]",
                "//div[contains(@class, 'search-btn')]",
                "//input[@type='submit']",
                "//button[contains(text(), '搜索')]",
                "//div[contains(@class, 'search-box')]//button"
            ]
            
            search_button = self._find_element_with_multiple_selectors(search_button_selectors)
            if not search_button:
                self.logger.warning("Could not find search button on homepage")
                return False
            
            # Click search button
            self._click_with_retry(search_button)
            time.sleep(5)
            
            # Check if search was successful
            if "search_result" in self.driver.current_url or "defaultresult" in self.driver.current_url:
                self.logger.info("Homepage search successful")
                return True
            else:
                self.logger.warning("Homepage search may have failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Homepage search error: {str(e)}")
            if self.debug_mode:
                self._inspect_page_for_debugging("homepage_search_error")
            return False
    
    def _try_advanced_search(self, term, db_code="CJFD"):
        """
        Try to search using the advanced search page
        
        Args:
            term: Search term
            db_code: Database code
            
        Returns:
            bool: Success indicator
        """
        try:
            self.logger.info(f"Attempting advanced search for term: {term}")
            
            # Navigate to advanced search page
            self.driver.get("https://kns.cnki.net/kns8/AdvSearch")
            time.sleep(3)
            
            if self.debug_mode:
                self._inspect_page_for_debugging("advanced_search_page")
            
            # Select database if needed
            if db_code != "CJFD":
                # Try to find and click database selector
                db_selector_selectors = [
                    "//div[contains(@class, 'database-box')]",
                    "//div[contains(@class, 'search-database')]",
                    "//div[contains(@class, 'custom-select')]"
                ]
                
                db_selector = self._find_element_with_multiple_selectors(db_selector_selectors)
                if db_selector:
                    self._click_with_retry(db_selector)
                    time.sleep(1)
                    
                    # Find and click the desired database option
                    db_option_selector = f"//li[@data-value='{db_code}'] | //div[@data-value='{db_code}']"
                    try:
                        db_option = WebDriverWait(self.driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, db_option_selector))
                        )
                        self._click_with_retry(db_option)
                        time.sleep(1)
                    except:
                        self.logger.warning(f"Could not select database {db_code}")
            
            # Find keyword input field
            keyword_field_selectors = [
                "//input[contains(@id, 'keyword')]",
                "//textarea[contains(@class, 'keyword')]",
                "//div[contains(@class, 'input-box')]//input",
                "//div[contains(@class, 'search-input')]//input"
            ]
            
            keyword_field = self._find_element_with_multiple_selectors(keyword_field_selectors)
            if not keyword_field:
                self.logger.warning("Could not find keyword field on advanced search page")
                return False
            
            # Clear and enter search term
            keyword_field.clear()
            keyword_field.send_keys(term)
            
            # Find and click search button
            search_button_selectors = [
                "//button[contains(text(), '搜索')]",
                "//button[contains(@class, 'search-btn')]",
                "//div[contains(@class, 'search-btn')]",
                "//input[@type='submit']"
            ]
            
            search_button = self._find_element_with_multiple_selectors(search_button_selectors)
            if not search_button:
                self.logger.warning("Could not find search button on advanced search page")
                return False
            
            # Click search button
            self._click_with_retry(search_button)
            time.sleep(5)
            
            # Check if search was successful
            if "search_result" in self.driver.current_url or "defaultresult" in self.driver.current_url:
                self.logger.info("Advanced search successful")
                return True
            else:
                self.logger.warning("Advanced search may have failed")
                return False
                
        except Exception as e:
            self.logger.error(f"Advanced search error: {str(e)}")
            if self.debug_mode:
                self._inspect_page_for_debugging("advanced_search_error")
            return False
    
    def _try_direct_url_search(self, term, db_code="CJFD"):
        """
        Try to search using direct URL construction
        
        Args:
            term: Search term
            db_code: Database code
            
        Returns:
            bool: Success indicator
        """
        try:
            self.logger.info(f"Attempting direct URL search for term: {term}")
            
            # Encode search term
            encoded_term = urllib.parse.quote(term)
            
            # Construct URL with different possible formats
            urls = [
                f"https://kns.cnki.net/kns8/defaultresult/index?kw={encoded_term}&korder=SU",
                f"https://kns.cnki.net/kns8/AdvSearch?dbprefix={db_code.lower()}&kw={encoded_term}",
                f"https://search.cnki.net/Search/Result?type=all&content={encoded_term}&dbcode={db_code}"
            ]
            
            for url_index, url in enumerate(urls):
                try:
                    self.logger.info(f"Trying URL format {url_index+1}: {url}")
                    self.driver.get(url)
                    time.sleep(5)
                    
                    # Check if search was successful
                    if "search_result" in self.driver.current_url or "defaultresult" in self.driver.current_url or "Result" in self.driver.current_url:
                        self.logger.info(f"Direct URL search successful with format {url_index+1}")
                        
                        if self.debug_mode:
                            self._inspect_page_for_debugging("direct_url_search_success")
                            
                        return True
                        
                except Exception as e:
                    self.logger.warning(f"URL format {url_index+1} failed: {str(e)}")
                    continue
            
            self.logger.warning("All direct URL search formats failed")
            return False
                
        except Exception as e:
            self.logger.error(f"Direct URL search error: {str(e)}")
            if self.debug_mode:
                self._inspect_page_for_debugging("direct_url_search_error")
            return False
    
    def direct_search_with_javascript(self, term, db_code="CJFD"):
        """
        Execute search using JavaScript injection
        
        Args:
            term: Search term
            db_code: Database code
            
        Returns:
            bool: Success indicator
        """
        try:
            self.logger.info(f"Attempting direct search with JavaScript for term: {term}")
            
            # Navigate to CNKI homepage
            self.driver.get("https://www.cnki.net/")
            time.sleep(3)
            
            # Encode the search term
            encoded_term = urllib.parse.quote(term)
            
            # Use JavaScript to navigate directly to the results page
            script = f"""
            window.location.href = 'https://kns.cnki.net/kns8/defaultresult/index?kw={encoded_term}';
            """
            
            self.driver.execute_script(script)
            time.sleep(5)
            
            # Take screenshot to verify results page
            if self.debug_mode:
                self._inspect_page_for_debugging("js_search_results")
            
            # Check if we successfully reached a results page
            if "defaultresult" in self.driver.current_url:
                self.logger.info("JavaScript search succeeded")
                return True
            else:
                self.logger.warning("JavaScript search may have failed")
                return False
                
        except Exception as e:
            self.logger.error(f"JavaScript search error: {str(e)}")
            return False
    
    def _try_http_fallback_search(self, term, db_code="CJFD", max_results=100, output_dir=None):
        """
        Fallback to simple HTTP request-based search when all Selenium methods fail
        """
        try:
            import requests
            from bs4 import BeautifulSoup
            
            self.logger.info(f"Using HTTP fallback search for term: {term}")
            
            output_dir = output_dir or self.output_dir
            results = []
            
            # Encoded search term
            encoded_term = urllib.parse.quote(term)
            
            # Construct search URL
            search_url = f"https://search.cnki.net/Search/Result?from=&t=dict&p={encoded_term}"
            
            # Use a realistic browser user agent
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"
            }
            
            # Make the request
            response = requests.get(search_url, headers=headers)
            
            if response.status_code == 200:
                # Parse the HTML
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Find result items
                item_selectors = [
                    "div.search-result div.search-result-item",
                    "div.result-list div.result-item",
                    "div.SearchResult div.ResultItem",
                    "tr.result-table-tr"
                ]
                
                for selector in item_selectors:
                    items = soup.select(selector)
                    if items:
                        break
                
                if not items:
                    self.logger.warning("HTTP fallback: No items found with standard selectors")
                    # Fall back to a more general approach - look for title links
                    items = soup.select("a[href*='dbcode=']")
                
                # Process items
                for item in items[:max_results]:
                    try:
                        # For title link fallback
                        if item.name == "a" and "title" not in item.get("class", []):
                            title = item.text.strip()
                            link = item.get("href", "")
                            
                            # Extract parent for other info
                            parent = item.parent
                            while parent and parent.name != "div" and parent.name != "tr":
                                parent = parent.parent
                            
                            if parent:
                                authors_elem = parent.select_one(".author, [class*='author']")
                                source_elem = parent.select_one(".source, [class*='source']")
                                date_elem = parent.select_one(".date, [class*='date']")
                                
                                authors = authors_elem.text.strip() if authors_elem else ""
                                source = source_elem.text.strip() if source_elem else ""
                                pub_date = date_elem.text.strip() if date_elem else ""
                            else:
                                authors = source = pub_date = ""
                        else:
                            # Standard item processing
                            title_elem = item.select_one(".title, .item-title, h3 a, [class*='title']")
                            title = title_elem.text.strip() if title_elem else ""
                            link = title_elem.get("href", "") if title_elem else ""
                            
                            authors_elem = item.select_one(".author, [class*='author']")
                            source_elem = item.select_one(".source, [class*='source'], .journal, [class*='journal']")
                            date_elem = item.select_one(".date, [class*='date'], .year, [class*='year']")
                            
                            authors = authors_elem.text.strip() if authors_elem else ""
                            source = source_elem.text.strip() if source_elem else ""
                            pub_date = date_elem.text.strip() if date_elem else ""
                        
                        # Add to results
                        if title:
                            results.append({
                                "title": title,
                                "authors": authors,
                                "source": source,
                                "publication_date": pub_date,
                                "link": link,
                                "database": "CNKI"
                            })
                    except Exception as e:
                        self.logger.warning(f"HTTP fallback: Error extracting item - {str(e)}")
            
            # Save results
            if results:
                # Create DataFrame and save
                df = pd.DataFrame(results)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(output_dir, f"cnki_http_fallback_{timestamp}.csv")
                json_path = os.path.join(output_dir, f"cnki_http_fallback_{timestamp}.json")
                
                df.to_csv(csv_path, index=False, encoding='utf-8')
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                
                return {
                    "status": "success", 
                    "count": len(results), 
                    "results": results,
                    "csv_path": csv_path,
                    "json_path": json_path,
                    "method": "http_fallback"
                }
            
            return {"status": "warning", "count": 0, "results": [], "method": "http_fallback"}
            
        except Exception as e:
            self.logger.error(f"HTTP fallback search error: {str(e)}")
            return {"status": "error", "message": str(e), "results": [], "method": "http_fallback"}
    
    def _ask_for_manual_mode(self):
        """Ask user if they want to use manual mode"""
        if not self.headless:
            try:
                import tkinter as tk
                from tkinter import messagebox
                
                root = tk.Tk()
                root.withdraw()  # Hide the main window
                
                response = messagebox.askyesno(
                    "Search Failed",
                    "All automatic search methods failed. Would you like to try manual collection mode?\n\n"
                    "This will open a browser window where you can manually perform the search."
                )
                
                root.destroy()
                return response
            except:
                # If tkinter fails, fall back to console input
                response = input("All automatic search methods failed. Try manual collection mode? (y/n): ")
                return response.lower().startswith('y')
        
        return False
    
    def manual_collection_mode(self, search_term, output_dir):
        """
        Enhanced manual collection mode with better user guidance
        """
        try:
            self.logger.info("Starting enhanced manual collection mode")
            
            # Create a browser instance with minimal automation flags
            chrome_options = Options()
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument('--ignore-certificate-errors')
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Open CNKI homepage
            driver.get("https://www.cnki.net/")
            
            # Create a simple GUI window with instructions using tkinter
            import tkinter as tk
            from tkinter import ttk, messagebox
            
            instruction_window = tk.Tk()
            instruction_window.title("CNKI Manual Collection Mode")
            instruction_window.geometry("600x500")
            
            # Instructions frame
            instruction_frame = ttk.Frame(instruction_window, padding=10)
            instruction_frame.pack(fill="both", expand=True)
            
            ttk.Label(instruction_frame, text="CNKI Manual Collection Instructions", font=("Arial", 14, "bold")).pack(pady=10)
            
            instructions = ttk.Frame(instruction_frame)
            instructions.pack(fill="both", expand=True)
            
            instruction_text = """
            1. A Chrome browser has opened to CNKI's homepage
            
            2. Please follow these steps:
               - Search for "{term}" using the search box
               - Login if necessary with your credentials
               - Navigate through search results pages
            
            3. When viewing a search results page, click the "Collect Current Page" button
            
            4. To collect from multiple pages, navigate to each page and click the collection button
            
            5. Click "Finish Collection" when done
            """.format(term=search_term)
            
            text_area = tk.Text(instructions, wrap="word", height=15)
            text_area.pack(fill="both", expand=True, padx=10, pady=10)
            text_area.insert("1.0", instruction_text)
            text_area.config(state="disabled")
            
            # Status label
            status_var = tk.StringVar(value="Ready to collect data")
            status_label = ttk.Label(instruction_frame, textvariable=status_var)
            status_label.pack(pady=5)
            
            # Results label
            results_var = tk.StringVar(value="Pages collected: 0")
            results_label = ttk.Label(instruction_frame, textvariable=results_var)
            results_label.pack(pady=5)
            
            # Collected results
            results_collected = []
            page_counter = [0]  # Use list to allow modification in nested function
            
            # Collection function
            def collect_current_page():
                try:
                    status_var.set("Collecting data from current page...")
                    instruction_window.update()
                    
                    # Check if we're on a search results page
                    if not any(x in driver.current_url for x in ['defaultresult', 'brief', 'search_result']):
                        status_var.set("Warning: Current page doesn't appear to be a search results page")
                        return
                    
                    # Find result items using various selectors
                    items = None
                    for selector in [
                        "//tr[contains(@class, 'result-table-tr')]",
                        "//div[contains(@class, 'result-item')]",
                        "//div[contains(@class, 'list-item')]",
                        "//table[@id='gridTable']//tr",
                        "//div[contains(@class, 'search-result')]//div[@data-index]"
                    ]:
                        items = driver.find_elements(By.XPATH, selector)
                        if items and len(items) > 0:
                            break
                    
                    if not items or len(items) == 0:
                        status_var.set("No results found on this page")
                        return
                    
                    # Process found items
                    page_results = []
                    for item in items:
                        try:
                            # Extract data with various selectors
                            title_element = None
                            for title_selector in [
                                ".//a[contains(@class, 'title')]",
                                ".//a[contains(@class, 'fz14')]",
                                ".//a[contains(@href, 'dbcode=')]",
                                ".//td[3]//a",  # Often the third column contains the title
                                ".//h3//a"
                            ]:
                                try:
                                    title_element = item.find_element(By.XPATH, title_selector)
                                    if title_element:
                                        break
                                except:
                                    continue
                            
                            if not title_element:
                                continue
                                
                            title = title_element.text.strip()
                            link = title_element.get_attribute("href") or ""
                            
                            # Extract authors
                            authors = ""
                            for authors_selector in [
                                ".//span[contains(@class, 'author')]",
                                ".//div[contains(@class, 'author')]",
                                ".//td[contains(@class, 'author')]"
                            ]:
                                try:
                                    authors_element = item.find_element(By.XPATH, authors_selector)
                                    authors = authors_element.text.strip()
                                    break
                                except:
                                    continue
                            
                            # Extract source
                            source = ""
                            for source_selector in [
                                ".//span[contains(@class, 'source')]",
                                ".//div[contains(@class, 'source')]",
                                ".//span[contains(@class, 'journal')]",
                                ".//div[contains(@class, 'journal')]"
                            ]:
                                try:
                                    source_element = item.find_element(By.XPATH, source_selector)
                                    source = source_element.text.strip()
                                    break
                                except:
                                    continue
                            
                            # Extract publication date
                            pub_date = ""
                            for date_selector in [
                                ".//span[contains(@class, 'date')]",
                                ".//div[contains(@class, 'date')]",
                                ".//span[contains(@class, 'year')]",
                                ".//div[contains(@class, 'year')]"
                            ]:
                                try:
                                    date_element = item.find_element(By.XPATH, date_selector)
                                    pub_date = date_element.text.strip()
                                    break
                                except:
                                    continue
                            
                            # Add to results
                            page_results.append({
                                "title": title,
                                "authors": authors,
                                "source": source,
                                "publication_date": pub_date,
                                "link": link,
                                "database": "CNKI"
                            })
                        except Exception as e:
                            print(f"Error extracting item: {str(e)}")
                    
                    # Add to collected results
                    results_collected.extend(page_results)
                    page_counter[0] += 1
                    
                    # Update status
                    status_var.set(f"Successfully collected {len(page_results)} items from page")
                    results_var.set(f"Pages collected: {page_counter[0]}, Total items: {len(results_collected)}")
                    
                    # Save current page results
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_path = os.path.join(output_dir, f"cnki_manual_page{page_counter[0]}_{timestamp}.csv")
                    pd.DataFrame(page_results).to_csv(csv_path, index=False, encoding='utf-8')
                    
                except Exception as e:
                    status_var.set(f"Error: {str(e)}")
            
            # Finish function
            def finish_collection():
                try:
                    if not results_collected:
                        status_var.set("No data collected yet")
                        return
                    
                    # Save all collected results
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_path = os.path.join(output_dir, f"cnki_manual_all_{timestamp}.csv")
                    json_path = os.path.join(output_dir, f"cnki_manual_all_{timestamp}.json")
                    
                    df = pd.DataFrame(results_collected)
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(results_collected, f, ensure_ascii=False, indent=2)
                    
                    status_var.set(f"Collection complete. Saved {len(results_collected)} items")
                    
                    # Ask user if they want to close the browser
                    if messagebox.askyesno("Collection Complete", 
                                          f"Collected {len(results_collected)} items.\nClose browser and collection window?"):
                        driver.quit()
                        instruction_window.destroy()
                except Exception as e:
                    status_var.set(f"Error saving results: {str(e)}")
            
            # Buttons
            button_frame = ttk.Frame(instruction_frame)
            button_frame.pack(pady=10)
            
            collect_button = ttk.Button(button_frame, text="Collect Current Page", command=collect_current_page)
            collect_button.pack(side="left", padx=10)
            
            finish_button = ttk.Button(button_frame, text="Finish Collection", command=finish_collection)
            finish_button.pack(side="left", padx=10)
            
            # Start the GUI loop
            instruction_window.mainloop()
            
            # Return results
            return {
                "status": "success",
                "results": results_collected,
            }
        
        except Exception as e:
            self.logger.error(f"Error in manual collection mode: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _is_no_results_page(self):
        """Check if we are on a 'no results' page"""
        try:
            no_results_indicators = [
                "//div[contains(text(), '抱歉，检索结果为空')]",
                "//div[contains(text(), '没有检索到相关结果')]",
                "//div[contains(@class, 'no-result')]",
                "//div[contains(@class, 'empty-result')]"
            ]
            
            for indicator in no_results_indicators:
                if self._is_element_present(By.XPATH, indicator, 1):
                    return True
            
            return False
        except:
            return False
    
    def _get_result_count(self):
        """Get the total count of search results"""
        try:
            count_selectors = [
                "//span[contains(@class, 'total')]/span",
                "//span[contains(@class, 'count')]/span",
                "//div[contains(text(), '共找到')]",
                "//div[contains(@class, 'search-count')]",
                "//div[contains(@class, 'result-count')]"
            ]
            
            for selector in count_selectors:
                try:
                    count_element = self.driver.find_element(By.XPATH, selector)
                    count_text = count_element.text.strip()
                    
                    # Extract number from text
                    import re
                    numbers = re.findall(r'\d+', count_text)
                    if numbers:
                        return int(numbers[0])
                except:
                    continue
            
            # If no count element found, try to count items directly
            try:
                # Try various selectors for result items
                for selector in [
                    "//tr[contains(@class, 'result-table-tr')]",
                    "//div[contains(@class, 'result-item')]",
                    "//div[contains(@class, 'list-item')]"
                ]:
                    items = self.driver.find_elements(By.XPATH, selector)
                    if items and len(items) > 0:
                        return len(items)
            except:
                pass
            
            return 0
        except Exception as e:
            self.logger.warning(f"Error getting result count: {str(e)}")
            return 0
    
    def _discover_results_page_structure(self):
        """
        Analyze the results page to determine its structure
        
        Returns:
            dict: Information about the page structure
        """
        try:
            structure = {"type": "unknown", "selectors": {}}
            
            # Check for common result page types
            # Type 1: Table-based results (older CNKI)
            if self._is_element_present(By.ID, "gridTable") or \
               self._is_element_present(By.XPATH, "//table[contains(@class, 'result-table')]"):
                structure["type"] = "table"
                structure["selectors"]["items"] = "//table[contains(@class, 'result-table')]/tbody/tr | //table[@id='gridTable']/tbody/tr"
                structure["selectors"]["title"] = ".//a[contains(@class, 'fz14') or contains(@class, 'title')]"
                structure["selectors"]["authors"] = ".//td[contains(@class, 'author')]"
                structure["selectors"]["source"] = ".//td[contains(@class, 'source') or contains(@class, 'journal')]"
                structure["selectors"]["date"] = ".//td[contains(@class, 'date') or contains(@class, 'year')]"
            
            # Type 2: Modern div-based results
            elif self._is_element_present(By.XPATH, "//div[contains(@class, 'result-item')]") or \
                 self._is_element_present(By.XPATH, "//div[contains(@class, 'search-result')]/div"):
                structure["type"] = "div"
                structure["selectors"]["items"] = "//div[contains(@class, 'result-item')] | //div[contains(@class, 'search-result')]/div[contains(@class, 'item')]"
                structure["selectors"]["title"] = ".//a[contains(@class, 'title')] | .//h3/a"
                structure["selectors"]["authors"] = ".//span[contains(@class, 'author')] | .//div[contains(@class, 'author')]"
                structure["selectors"]["source"] = ".//span[contains(@class, 'source')] | .//div[contains(@class, 'source')]"
                structure["selectors"]["date"] = ".//span[contains(@class, 'date')] | .//div[contains(@class, 'date')]"
            
            # Type 3: New CNKI interface structure
            elif self._is_element_present(By.XPATH, "//div[contains(@class, 'search-reulst-list')]") or \
                 self._is_element_present(By.XPATH, "//div[contains(@class, 'result-list')]"):
                structure["type"] = "new"
                structure["selectors"]["items"] = "//div[contains(@class, 'result-list-item')] | //div[contains(@class, 'result-item')]"
                structure["selectors"]["title"] = ".//a[contains(@data-action, 'article')] | .//a[contains(@class, 'text')]"
                structure["selectors"]["authors"] = ".//div[contains(@class, 'authors')] | .//div[contains(@class, 'author')]"
                structure["selectors"]["source"] = ".//div[contains(@class, 'source')] | .//span[contains(@class, 'journal')]"
                structure["selectors"]["date"] = ".//span[contains(@class, 'year')] | .//span[contains(@class, 'date')]"
            
            # If no known structure is detected, try a generic approach
            if structure["type"] == "unknown":
                self.logger.warning("Unknown results page structure, using generic selectors")
                structure["type"] = "generic"
                
                # Find potential result items by looking for repeating elements
                potential_items = []
                for selector in [
                    "//div[contains(@class, 'result')]/div",
                    "//div[contains(@class, 'list')]/div",
                    "//table//tr[position()>1]",  # Skip header row
                    "//div[contains(@class, 'item')]"
                ]:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    if len(elements) >= 3:  # At least 3 items to be considered a result list
                        potential_items.append((selector, len(elements)))
                
                # Use the selector with the most items
                if potential_items:
                    potential_items.sort(key=lambda x: x[1], reverse=True)
                    structure["selectors"]["items"] = potential_items[0][0]
                    self.logger.info(f"Found potential result items using selector: {potential_items[0][0]}")
                    
                    # Get one item to analyze
                    item = self.driver.find_element(By.XPATH, structure["selectors"]["items"])
                    
                    # Find potential title element (usually an anchor tag)
                    anchor_tags = item.find_elements(By.TAG_NAME, "a")
                    if anchor_tags:
                        # The first or largest anchor tag is often the title
                        structure["selectors"]["title"] = ".//a"
                    else:
                        structure["selectors"]["title"] = ".//*[contains(@class, 'title')]"
                    
                    # Generic selectors for other fields
                    structure["selectors"]["authors"] = ".//*[contains(@class, 'author')]"
                    structure["selectors"]["source"] = ".//*[contains(@class, 'source') or contains(@class, 'journal')]"
                    structure["selectors"]["date"] = ".//*[contains(@class, 'date') or contains(@class, 'year') or contains(@class, 'time')]"
                else:
                    self.logger.error("Could not determine results page structure")
                    return None
            
            return structure
            
        except Exception as e:
            self.logger.error(f"Error discovering results page structure: {str(e)}")
            return None
    
    def _extract_items_with_structure(self, structure):
        """
        Extract result items using the discovered structure
        
        Args:
            structure: Page structure information
            
        Returns:
            list: WebElement objects representing result items
        """
        try:
            items_selector = structure["selectors"]["items"]
            items = self.driver.find_elements(By.XPATH, items_selector)
            
            self.logger.info(f"Found {len(items)} items using selector: {items_selector}")
            
            # If we found header rows in a table, remove them
            if structure["type"] == "table" and len(items) > 0:
                # Check if first row is a header row
                first_item = items[0]
                if first_item.find_elements(By.TAG_NAME, "th") or 'class' in first_item.get_attribute('outerHTML') and 'header' in first_item.get_attribute('class'):
                    items = items[1:]
                    self.logger.info(f"Removed header row, {len(items)} items remaining")
            
            return items
            
        except Exception as e:
            self.logger.error(f"Error extracting items with structure: {str(e)}")
            return []
    
    def _extract_data_from_item(self, item, structure):
        """
        Extract data from a single result item
        
        Args:
            item: WebElement representing a result item
            structure: Page structure information
            
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
            
            # Get selectors from structure
            title_selector = structure["selectors"]["title"]
            authors_selector = structure["selectors"]["authors"]
            source_selector = structure["selectors"]["source"]
            date_selector = structure["selectors"]["date"]
            
            # Extract title and link
            title_element = None
            try:
                title_element = item.find_element(By.XPATH, title_selector)
            except:
                # Try a more general approach
                anchor_elements = item.find_elements(By.TAG_NAME, "a")
                # Find the anchor element with the most text
                if anchor_elements:
                    title_element = max(anchor_elements, key=lambda e: len(e.text.strip() or ""))
            
            if not title_element:
                self.logger.warning("Could not find title element, skipping item")
                return None
                
            title = title_element.text.strip()
            link = title_element.get_attribute("href") or ""
            
            # Extract other fields
            authors = ""
            source = ""
            pub_date = ""
            
            try:
                authors_element = item.find_element(By.XPATH, authors_selector)
                authors = authors_element.text.strip()
            except:
                pass
                
            try:
                source_element = item.find_element(By.XPATH, source_selector)
                source = source_element.text.strip()
            except:
                pass
                
            try:
                date_element = item.find_element(By.XPATH, date_selector)
                pub_date = date_element.text.strip()
            except:
                pass
            
            # Return data if we at least have a title
            if title:
                return {
                    "title": title,
                    "authors": authors,
                    "source": source,
                    "publication_date": pub_date,
                    "link": link,
                    "database": "CNKI"
                }
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"Error extracting data from item: {str(e)}")
            return None
    
    def _go_to_next_page_adaptive(self):
        """
        Navigate to the next page of results with adaptive selector detection
        
        Returns:
            bool: True if successfully navigated to next page, False otherwise
        """
        try:
            # Take screenshot before pagination
            if self.debug_mode:
                self._inspect_page_for_debugging("before_pagination")
            
            # Find all potential next page buttons
            potential_next_buttons = []
            
            # Classic selectors
            for selector in [
                "//a[@id='PageNext']",
                "//a[contains(@class, 'next')]",
                "//a[contains(text(), '下一页')]",
                "//a[text()='›']",
                "//a[text()='>']",
                "//a[contains(@href, 'page=') and (contains(@class, 'next') or contains(@onclick, 'next'))]"
            ]:
                elements = self.driver.find_elements(By.XPATH, selector)
                for element in elements:
                    if element.is_displayed():
                        # Check if the button is disabled
                        classes = element.get_attribute("class") or ""
                        if "disabled" not in classes and "last" not in classes:
                            potential_next_buttons.append((element, selector))
            
            # If no classic next buttons found, look for paging controls more generically
            if not potential_next_buttons:
                # Look for pagination area
                pagination_areas = self.driver.find_elements(By.XPATH, 
                    "//div[contains(@class, 'pager') or contains(@class, 'pagination')]")
                
                if pagination_areas:
                    # Find all links in the pagination area
                    for area in pagination_areas:
                        links = area.find_elements(By.TAG_NAME, "a")
                        # Current page number
                        current_page_elements = area.find_elements(By.XPATH, 
                            ".//*[contains(@class, 'current') or contains(@class, 'active')]")
                        
                        current_page = 1
                        if current_page_elements:
                            try:
                                current_page = int(current_page_elements[0].text.strip())
                            except:
                                pass
                        
                        # Find the link with text or class suggesting it's the next page
                        for link in links:
                            link_text = link.text.strip()
                            try:
                                # If it's a numbered link and one more than current page
                                if link_text.isdigit() and int(link_text) == current_page + 1:
                                    potential_next_buttons.append((link, "next page number"))
                                    break
                            except:
                                pass
                            
                            # Look for arrow or next text
                            if link_text in ['›', '>', '下一页', 'Next', 'next']:
                                potential_next_buttons.append((link, "next page text"))
                                break
            
            # Use JavaScript to look for the next page element if nothing found yet
            if not potential_next_buttons:
                self.logger.info("No standard next page buttons found, trying JavaScript approach")
                script = """
                function findNextPageElement() {
                    // Look for elements containing text indicative of next page
                    var nextTexts = ['下一页', '下页', '下一頁', 'Next', 'next', '>', '›'];
                    for (var i = 0; i < nextTexts.length; i++) {
                        var text = nextTexts[i];
                        var elements = Array.from(document.querySelectorAll('a, button, span, div'))
                            .filter(el => el.textContent.includes(text) || 
                                        (el.getAttribute('title') && el.getAttribute('title').includes(text)));
                        
                        if (elements.length > 0) {
                            return elements[0];
                        }
                    }
                    
                    // Look for links with href containing page=, pageNum=, etc.
                    var pageLinks = Array.from(document.querySelectorAll('a[href*="page="], a[href*="pageNum="], a[href*="PageIndex="]'));
                    var currentPageNum = null;
                    
                    // Try to find current page number
                    var currentElements = document.querySelectorAll('.current, .active, [class*="current"], [class*="active"]');
                    for (var i = 0; i < currentElements.length; i++) {
                        var num = parseInt(currentElements[i].textContent.trim());
                        if (!isNaN(num)) {
                            currentPageNum = num;
                            break;
                        }
                    }
                    
                    if (currentPageNum !== null) {
                        // Find link to next page number
                        for (var i = 0; i < pageLinks.length; i++) {
                            var num = parseInt(pageLinks[i].textContent.trim());
                            if (!isNaN(num) && num === currentPageNum + 1) {
                                return pageLinks[i];
                            }
                        }
                    }
                    
                    return null;
                }
                return findNextPageElement();
                """
                
                next_element = self.driver.execute_script(script)
                if next_element:
                    potential_next_buttons.append((next_element, "JavaScript finder"))
            
            # Try clicking the best candidate
            if potential_next_buttons:
                self.logger.info(f"Found {len(potential_next_buttons)} potential next page buttons")
                
                next_button, selector = potential_next_buttons[0]
                self.logger.info(f"Attempting to click next page button found with {selector}")
                
                # Use JavaScript click to avoid possible element visibility issues
                self.driver.execute_script("arguments[0].click();", next_button)
                
                # Wait for page to load
                time.sleep(3)
                
                # Check if URL or page content changed
                # Take screenshot after pagination
                if self.debug_mode:
                    self._inspect_page_for_debugging("after_pagination")
                
                return True
            else:
                self.logger.info("No next page button found")
                return False
            
        except Exception as e:
            self.logger.warning(f"Error navigating to next page: {str(e)}")
            return False
    
    def _adaptive_result_collection(self, max_results):
        """
        Collect results using adaptive methods that detect page structure
        """
        results = []
        current_page = 1
        
        while len(results) < max_results:
            self.logger.info(f"Processing page {current_page}")
            
            # Take screenshot of current page for debugging
            if self.debug_mode:
                self._inspect_page_for_debugging(f"results_page_{current_page}")
            
            # First, try to discover the structure of the results page
            page_structure = self._discover_results_page_structure()
            
            if not page_structure:
                self.logger.warning(f"Could not determine results page structure on page {current_page}")
                break
            
            # Extract items using the discovered structure
            items = self._extract_items_with_structure(page_structure)
            
            if not items or len(items) == 0:
                self.logger.warning(f"No items found on page {current_page}")
                break
            
            # Process each item
            for item in items:
                if len(results) >= max_results:
                    break
                    
                result = self._extract_data_from_item(item, page_structure)
                if result:
                    results.append(result)
            
            # Check if we need to go to next page
            if len(results) < max_results:
                if not self._go_to_next_page_adaptive():
                    self.logger.info("No more pages available")
                    break
                current_page += 1
                time.sleep(2)
        
        self.logger.info(f"Collected {len(results)} results from {current_page} pages")
        return results
    
    def search_and_collect(self, term, date_range=None, max_results=100, db_code="CJFD"):
        """
        Enhanced search with multiple fallback mechanisms
        
        Args:
            term (str): Search term
            date_range (tuple): Date range as (start_date, end_date)
            max_results (int): Maximum number of results to collect
            db_code (str): Database code
            
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
            
            # Attempt search using multiple methods in order
            search_methods = [
                self._try_homepage_search,
                self._try_advanced_search,
                self._try_direct_url_search,
                self.direct_search_with_javascript,
                self._try_http_fallback_search
            ]
            
            search_successful = False
            method_name = None
            
            for method_idx, search_method in enumerate(search_methods):
                method_name = search_method.__name__
                self.logger.info(f"Attempting search method {method_idx+1}/{len(search_methods)}: {method_name}")
                
                # Try this search method
                if method_name == "_try_http_fallback_search":
                    # HTTP fallback uses a different approach
                    results = search_method(term, db_code, max_results, self.output_dir)
                    if results and results.get("status") in ["success", "warning"]:
                        self.logger.info(f"HTTP fallback search succeeded with {len(results.get('results', []))} results")
                        return results
                else:
                    # Regular Selenium search method
                    success = search_method(term, db_code)
                    if success:
                        search_successful = True
                        self.logger.info(f"Search method succeeded: {method_name}")
                        break
            
            # If all search methods failed, suggest manual mode
            if not search_successful and not method_name == "_try_http_fallback_search":
                self.logger.error("All automatic search methods failed")
                self.logger.info("Suggesting manual collection mode")
                
                if self._ask_for_manual_mode():
                    return self.manual_collection_mode(term, self.output_dir)
                else:
                    return {"status": "error", "message": "All search methods failed", "results": []}
            
            # If we got here with search_successful, proceed with collecting results
            self.logger.info("Search successful, collecting results")
            
            # Take screenshot of search results page for debugging
            if self.debug_mode:
                self._inspect_page_for_debugging("successful_search_results")
            
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
            
            # Collect results using adaptive methods
            results = self._adaptive_result_collection(max_results)
            
            # Save results
            if results:
                # Create DataFrame
                df = pd.DataFrame(results)
                
                # Save as CSV and JSON
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_path = os.path.join(self.output_dir, f"cnki_results_{db_code}_{timestamp}.csv")
                json_path = os.path.join(self.output_dir, f"cnki_results_{db_code}_{timestamp}.json")
                
                df.to_csv(csv_path, index=False, encoding='utf-8')
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                
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
    
    def close(self):
        """Clean up resources"""
        if hasattr(self, 'driver') and self.driver:
            self.logger.info("Closing browser")
            self.driver.quit()


# Example usage
if __name__ == "__main__":
    # Create CNKI scraper instance
    scraper = CNKIWebScraper(
        username="your_username",  # Optional
        password="your_password",  # Optional
        output_dir="./cnki_results",
        debug_mode=True  # Enable debugging for development
    )
    
    try:
        # Search and collect results
        results = scraper.search_and_collect(
            term="矽肺",  # Search term
            max_results=50,  # Maximum number of results to collect
            db_code="CJFD"  # Database code: CJFD (journals), CDFD (PhD theses), CMFD (Masters theses)
        )
        
        print(f"Search status: {results['status']}")
        print(f"Total results found: {results.get('count', 0)}")
        print(f"Results collected: {len(results.get('results', []))}")
        
        if 'csv_path' in results:
            print(f"Results saved to: {results['csv_path']}")
            
    finally:
        # Always close the browser
        scraper.close()
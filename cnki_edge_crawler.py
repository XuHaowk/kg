#!/usr/bin/env python3
"""
CNKI Edge Browser Crawler

This module provides a Microsoft Edge-based crawler for the CNKI (China National Knowledge
Infrastructure) website, utilizing Selenium for browser automation.
"""

import os
import sys
import time
import json
import re
import logging
import concurrent.futures
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains

class CNKIEdgeCrawler:
    """CNKI crawler using Edge browser with Selenium"""
    
    def __init__(self, output_dir="output"):
        """
        Initialize the CNKI Edge crawler
        
        Args:
            output_dir (str): Output directory path
        """
        self.output_dir = output_dir
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Configure logging
        self.logger = self._setup_logger()
        
        # Configure more detailed logging for debugging
        self.debug_log_path = os.path.join(self.output_dir, f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        debug_handler = logging.FileHandler(self.debug_log_path, encoding='utf-8')
        debug_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        debug_handler.setFormatter(formatter)
        self.logger.addHandler(debug_handler)
    
    def _setup_logger(self):
        """Set up logger"""
        logger = logging.getLogger("CNKIEdgeCrawler")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create file handler
        log_path = os.path.join(self.output_dir, f"cnki_edge_crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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
    
    def retry_operation(self, operation, max_attempts=3, delay=2):
        """
        Retry an operation multiple times with delay
        
        Args:
            operation: Function to retry
            max_attempts: Maximum number of retry attempts
            delay: Base delay between attempts (increases with each attempt)
            
        Returns:
            Result of the operation or raises the last exception
        """
        last_exception = None
        for attempt in range(max_attempts):
            try:
                return operation()
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Attempt {attempt+1} failed: {str(e)}")
                time.sleep(delay * (attempt + 1))  # Increasing delay
        
        raise last_exception  # Re-raise the last exception if all attempts failed
    
    def setup_driver(self):
        """Set up Microsoft Edge driver"""
        # Get direct return, don't wait for page to finish loading
        desired_capabilities = DesiredCapabilities.EDGE
        desired_capabilities["pageLoadStrategy"] = "none"

        # Set up Microsoft Edge driver environment
        options = webdriver.EdgeOptions()
        
        # Set browser not to load images to improve speed
        options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
        
        # Add anti-detection measures
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Add language and encoding settings
        options.add_argument("--lang=zh-CN")
        options.add_argument("--accept-charset=UTF-8")

        # Create Microsoft Edge driver
        driver = webdriver.Edge(options=options)
        
        # Additional anti-detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Set window size
        driver.set_window_size(1366, 768)
        
        return driver
    
    def open_search_page(self, driver, keyword):
        """
        Open the search page and input the keyword
        
        Args:
            driver: Selenium webdriver
            keyword: Search keyword
            
        Returns:
            int: Number of results found
        """
        # Open page and wait for complete load
        driver.get("https://kns.cnki.net/kns8/AdvSearch")
        
        # Wait for page to be interactive
        time.sleep(5)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.search-main')))
        
        # Close any popups if they exist
        try:
            popup_close = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.layui-layer-close'))
            )
            popup_close.click()
            time.sleep(1)
        except:
            pass  # No popup
        
        # Try to modify attribute to show dropdown
        try:
            opt = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.sort-list'))
            )
            driver.execute_script("arguments[0].setAttribute('style', 'display: block;')", opt)
            
            # Move mouse to [Corresponding Author] in dropdown
            author_option = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'li[data-val="RP"]'))
            )
            ActionChains(driver).move_to_element(author_option).perform()
            author_option.click()
            time.sleep(1)
        except Exception as e:
            self.logger.warning(f"Could not select author dropdown: {str(e)}")
            # Continue with default search fields
        
        # Input keyword in search field
        search_input = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//input[@class="ipt-txt"]'))
        )
        search_input.clear()
        search_input.send_keys(keyword)
        time.sleep(1)
        
        # Click search button
        search_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//button[@class="btn-search"]'))
        )
        search_button.click()
        
        self.logger.info("Searching, please wait...")
        
        # Wait for results page to load
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "gridTable"))
        )
        time.sleep(3)  # Additional wait for results to populate
        
        # Get total number of papers
        try:
            res_count_element = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, '//span[@class="pager_count"]/em'))
            )
            res_unm = res_count_element.text.strip()
            # Remove commas from thousands
            res_unm = int(res_unm.replace(",", ''))
            page_unm = (res_unm // 20) + (1 if res_unm % 20 > 0 else 0)
            self.logger.info(f"Found {res_unm} results, {page_unm} pages.")
            return res_unm
        except Exception as e:
            self.logger.error(f"Could not get result count: {str(e)}")
            # Try an alternative method to get count
            try:
                result_text = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="search-result"]'))
                ).text
                match = re.search(r'(\d+(?:,\d+)*)', result_text)
                if match:
                    res_unm = int(match.group(1).replace(",", ""))
                    page_unm = (res_unm // 20) + (1 if res_unm % 20 > 0 else 0)
                    self.logger.info(f"Found {res_unm} results, {page_unm} pages.")
                    return res_unm
            except:
                pass
            
            # Return default if we couldn't get the real count
            self.logger.warning("Could not determine result count, using default")
            return 100
    
    def get_info(self, driver, xpath):
        """Helper function to get text from element by xpath"""
        try:
            # Increase wait time and handle dynamic loading
            element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, xpath)))
            return element.text.strip() or "无"
        except Exception as e:
            self.logger.debug(f"Element not found with xpath: {xpath}, error: {str(e)}")
            return '无'

    def get_choose_info(self, driver, xpath1, xpath2, str):
        """Helper function to get specific information"""
        try:
            # Increase wait time for better reliability
            if WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, xpath1))).text.strip() == str:
                return WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, xpath2))).text.strip() or "无"
            else:
                return '无'
        except Exception as e:
            self.logger.debug(f"Could not get specific info, error: {str(e)}")
            return '无'
    
    def crawl_articles(self, driver, papers_need, theme, output_file=None):
        """
        Crawl articles from CNKI
        
        Args:
            driver: Selenium webdriver
            papers_need: Number of papers to crawl
            theme: Search theme/keyword
            output_file: Output file path (optional)
            
        Returns:
            list: Crawled articles data
        """
        count = 1
        articles = []
        
        # Default output file
        if output_file is None:
            output_file = os.path.join(self.output_dir, f"{theme}.tsv")
        
        # Check if file exists and read last record count
        if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
            with open(output_file, "r", encoding='utf-8') as file:
                lines = file.readlines()
                if lines:
                    last_line = lines[-1].strip()
                    try:
                        count = int(last_line.split("\t")[0]) + 1
                    except (ValueError, IndexError):
                        count = 1
        
        # Skip to the correct page
        current_page = 1
        target_page = ((count - 1) // 20) + 1
        
        while current_page < target_page:
            # Click the next page button
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[@id='PageNext' or @class='next']"))
                )
                next_button.click()
                time.sleep(3)
                current_page += 1
                self.logger.info(f"Moving to page {current_page}...")
            except Exception as e:
                self.logger.error(f"Error navigating to next page: {str(e)}")
                break
        
        self.logger.info(f"Starting from record {count}\n")
        
        # While crawled count is less than needed
        while count <= papers_need:
            # Wait for loading
            time.sleep(3)
            
            try:
                # Get all article rows in the current page
                article_rows = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//table[@class='result-table-list']/tbody/tr"))
                )
                
                # Calculate which items on this page to process
                start_item = (count - 1) % 20 + 1
                end_item = min(len(article_rows) + 1, 21)  # +1 because of 1-indexing
                
                for i in range(start_item, end_item):
                    if count > papers_need:
                        break
                    
                    self.logger.info(f"\n###Crawling item {count} (Page {(count - 1) // 20 + 1}, Item {i})#######################################\n")
                    
                    try:
                        # Find the title link for this item and click it
                        title_element = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, f"//table[@class='result-table-list']/tbody/tr[{i}]/td[2]//a"))
                        )
                        
                        # Get basic info before clicking
                        self.logger.info('Getting basic info...')
                        
                        # Get title
                        title = title_element.text.strip()
                        
                        # Get authors
                        try:
                            authors = driver.find_element(By.XPATH, f"//table[@class='result-table-list']/tbody/tr[{i}]/td[3]").text.strip()
                        except:
                            authors = "无"
                        
                        # Get source
                        try:
                            source = driver.find_element(By.XPATH, f"//table[@class='result-table-list']/tbody/tr[{i}]/td[4]").text.strip()
                        except:
                            source = "无"
                        
                        # Get date
                        try:
                            date = driver.find_element(By.XPATH, f"//table[@class='result-table-list']/tbody/tr[{i}]/td[5]").text.strip()
                        except:
                            date = "无"
                        
                        # Get database
                        try:
                            database = driver.find_element(By.XPATH, f"//table[@class='result-table-list']/tbody/tr[{i}]/td[6]").text.strip()
                        except:
                            database = "无"
                        
                        # Get citations
                        try:
                            quote_element = driver.find_element(By.XPATH, f"//table[@class='result-table-list']/tbody/tr[{i}]/td[7]")
                            quote = quote_element.text.strip()
                            if not quote.isdigit():
                                quote = '0'
                        except:
                            quote = '0'
                        
                        # Get downloads
                        try:
                            download_element = driver.find_element(By.XPATH, f"//table[@class='result-table-list']/tbody/tr[{i}]/td[8]")
                            download = download_element.text.strip()
                            if not download.isdigit():
                                download = '0'
                        except:
                            download = '0'
                        
                        self.logger.info(f"{title} {authors} {source} {date} {database} {quote} {download}\n")
                        
                        # Click on the title to open the detailed page
                        title_element.click()
                        
                        # Wait for new tab/window to open
                        time.sleep(5)
                        
                        # Switch to the new window
                        window_handles = driver.window_handles
                        if len(window_handles) > 1:
                            driver.switch_to.window(window_handles[-1])
                        else:
                            self.logger.warning("New window did not open, attempting to continue...")
                        
                        # Wait for page to load
                        WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.XPATH, "//div[@class='doc'] | //div[@class='brief']"))
                        )
                        
                        # Get author institution
                        self.logger.info('Getting institute...')
                        try:
                            institute = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, "//h3[contains(text(), '作者')]/following-sibling::div[1] | //div[contains(@class, 'author')]"))
                            ).text.strip()
                        except:
                            institute = '无'
                        self.logger.info(institute + '\n')
                        
                        # Get abstract
                        self.logger.info('Getting abstract...')
                        try:
                            # Try to expand abstract if it's collapsed
                            try:
                                expand_button = WebDriverWait(driver, 3).until(
                                    EC.element_to_be_clickable((By.XPATH, "//a[@id='ChDivSummaryMore' or contains(@class, 'abstract-more')]"))
                                )
                                expand_button.click()
                                time.sleep(0.5)
                            except:
                                pass  # No expand button or already expanded
                                
                            abstract = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'abstract-text')] | //div[@id='ChDivSummary']"))
                            ).text.strip()
                        except:
                            abstract = '无'
                        self.logger.info(abstract + '\n')
                        
                        # Get keywords
                        self.logger.info('Getting keywords...')
                        try:
                            keywords = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'keywords')] | //p[@class='keywords']"))
                            ).text.strip()
                        except:
                            keywords = '无'
                        self.logger.info(keywords + '\n')
                        
                        # Get publication
                        self.logger.info('Getting publication...')
                        publication = '无'
                        try:
                            # Try different possible XPaths
                            pub_elements = driver.find_elements(By.XPATH, "//li/span[contains(text(), '专辑')]/../p")
                            if pub_elements:
                                publication = pub_elements[0].text.strip()
                        except:
                            pass
                        self.logger.info(publication + '\n')
                        
                        # Get topic
                        self.logger.info('Getting topic...')
                        topic = '无'
                        try:
                            # Try different possible XPaths
                            topic_elements = driver.find_elements(By.XPATH, "//li/span[contains(text(), '专题')]/../p")
                            if topic_elements:
                                topic = topic_elements[0].text.strip()
                        except:
                            pass
                        self.logger.info(topic + '\n')
                        
                        # Get current URL
                        url = driver.current_url
                        
                        # Prepare article data
                        article_data = {
                            "id": count,
                            "title": title,
                            "authors": authors,
                            "institute": institute,
                            "date": date,
                            "source": source,
                            "publication": publication,
                            "topic": topic,
                            "database": database,
                            "quote": quote,
                            "download": download,
                            "keywords": keywords,
                            "abstract": abstract,
                            "url": url
                        }
                        articles.append(article_data)
                        
                        # Format TSV line
                        res = f"{count}\t{title}\t{authors}\t{institute}\t{date}\t{source}\t{publication}\t{topic}\t{database}\t{quote}\t{download}\t{keywords}\t{abstract}\t{url}".replace(
                            "\n", "") + "\n"
                        
                        # Write to file with proper encoding
                        try:
                            with open(output_file, 'a', encoding='utf-8') as f:
                                f.write(res)
                                self.logger.info('Write successful')
                        except Exception as e:
                            self.logger.error(f'Write failed: {str(e)}')
                            raise e
                        
                    except Exception as e:
                        self.logger.error(f"Error crawling item {count}: {str(e)}")
                        # Add error record to help with debugging
                        error_data = {
                            "id": count,
                            "error": str(e),
                            "url": driver.current_url
                        }
                        articles.append(error_data)
                    
                    finally:
                        # If there are multiple windows, close the detail page and switch back to results
                        window_handles = driver.window_handles
                        if len(window_handles) > 1:
                            driver.close()
                            driver.switch_to.window(window_handles[0])
                        
                        # Increment count
                        count += 1
                        if count > papers_need:
                            break
                
                # Go to next page if needed
                if count <= papers_need:
                    try:
                        next_button = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, "//a[@id='PageNext' or @class='next']"))
                        )
                        next_button.click()
                        time.sleep(3)
                    except Exception as e:
                        self.logger.warning(f"No more pages or error going to next page: {str(e)}")
                        break
                    
            except Exception as e:
                self.logger.error(f"Error processing page: {str(e)}")
                break
        
        self.logger.info("Crawling completed!")
        
        # Save to JSON for system integration
        json_path = os.path.join(self.output_dir, f"{theme}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        
        # Also save as CSV for easier viewing
        csv_path = os.path.join(self.output_dir, f"{theme}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        try:
            import pandas as pd
            df = pd.DataFrame(articles)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')  # utf-8-sig for Excel compatibility
        except Exception as e:
            self.logger.error(f"Error creating CSV: {str(e)}")
        
        return articles
    
    def search_cnki(self, term, date_range=None, max_results=100, db_code="CJFD"):
        """
        Search CNKI literature and download results
        
        Args:
            term (str): Search term
            date_range (tuple): Date range in format (start_date, end_date), e.g. ("2020/01/01", "2023/12/31")
            max_results (int): Maximum number of results to collect
            db_code (str): Database code, CJFD for journals, CDFD for PhD theses, CMFD for Master theses
            
        Returns:
            dict: Dictionary containing search results
        """
        self.logger.info(f"Starting CNKI search for '{term}'")
        
        # Create Edge driver
        driver = self.setup_driver()
        
        try:
            # Set page load timeout
            driver.set_page_load_timeout(60)
            
            # Open search page and get the number of results
            res_unm = self.open_search_page(driver, term)
            
            # Determine how many papers to crawl
            papers_need = min(max_results, res_unm)
            
            # Wait for manual interaction - this is a critical step for CNKI
            input("Please check the search results and press Enter to continue...")
            
            # Start crawling
            output_file = os.path.join(self.output_dir, f"{term}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tsv")
            results = self.crawl_articles(driver, papers_need, term, output_file)
            
            self.logger.info(f"Search completed. Crawled {len(results)} articles out of {res_unm} results.")
            
            # Save as JSON format that's compatible with the system
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.json")
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            # Convert to DataFrame for better compatibility with the rest of the system
            import pandas as pd
            df = pd.DataFrame(results)
            csv_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')  # Use utf-8-sig for Excel compatibility
            
            return {"count": res_unm, "results": results, "json_path": json_path, "csv_path": csv_path}
            
        except Exception as e:
            self.logger.error(f"Error during CNKI search: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {"error": str(e)}
        finally:
            # Close the browser
            try:
                driver.quit()
            except:
                pass

# If run as script
if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="CNKI Edge Browser Crawler")
    parser.add_argument("term", help="Search term")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--max-results", type=int, default=100, help="Maximum number of results to collect")
    parser.add_argument("--db-code", default="CJFD", choices=["CJFD", "CDFD", "CMFD"], 
                       help="Database code: CJFD (journals), CDFD (PhD theses), CMFD (Master theses)")
    
    args = parser.parse_args()
    
    # Create crawler and run search
    crawler = CNKIEdgeCrawler(output_dir=args.output_dir)
    results = crawler.search_cnki(
        term=args.term,
        max_results=args.max_results,
        db_code=args.db_code
    )
    
    # Print summary
    if "error" in results:
        print(f"Error: {results['error']}")
    else:
        print(f"Search completed. Found {results['count']} results, crawled {len(results['results'])} articles.")
        print(f"Results saved to {results.get('json_path', '')} and {results.get('csv_path', '')}")
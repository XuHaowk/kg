#!/usr/bin/env python3
"""
CNKI Undetected Chrome Crawler

This module provides a specialized crawler for the CNKI (China National Knowledge
Infrastructure) website that bypasses anti-bot measures using undetected-chromedriver.
"""

import os
import sys
import time
import json
import re
import logging
import random
import pandas as pd
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

class CNKIUndetectedCrawler:
    """CNKI crawler using undetected-chromedriver for bypassing anti-bot measures"""
    
    def __init__(self, output_dir="output", headless=False):
        """
        Initialize the CNKI Undetected crawler
        
        Args:
            output_dir (str): Output directory path
            headless (bool): Whether to run in headless mode (no visible browser)
        """
        self.output_dir = output_dir
        self.headless = headless
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Configure logging
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """Set up logger"""
        logger = logging.getLogger("CNKIUndetectedCrawler")
        logger.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create file handler
        log_path = os.path.join(self.output_dir, f"cnki_crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
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
    
    def human_like_delay(self, min_seconds=1, max_seconds=3):
        """Add a random delay to simulate human behavior"""
        time.sleep(random.uniform(min_seconds, max_seconds))
    
    def setup_driver(self):
        """Set up undetected ChromeDriver"""
        options = uc.ChromeOptions()
        
        # Add language and encoding settings
        options.add_argument("--lang=zh-CN")
        
        # Set user agent to appear more like a real browser
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        options.add_argument(f"--user-agent={user_agent}")
        
        # Set screen size
        options.add_argument("--window-size=1920,1080")
        
        # Do not load images if needed for speed
        # options.add_argument("--blink-settings=imagesEnabled=false")
        
        # Create and return the driver
        driver = uc.Chrome(options=options, headless=self.headless)
        
        # Set default timeout
        driver.set_page_load_timeout(60)
        
        return driver
    
    def navigate_to_search_page(self, driver):
        """Navigate to CNKI advanced search page"""
        try:
            # Go to CNKI home page first (more reliable)
            driver.get("https://www.cnki.net/")
            self.human_like_delay(2, 4)
            
            # Look for and click the advanced search link
            adv_search_link = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), '高级检索') or contains(@href, 'AdvSearch')]"))
            )
            adv_search_link.click()
            self.human_like_delay()
            
            # Wait for the advanced search page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "gradetxt"))
            )
            self.logger.info("Advanced search page loaded successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to navigate to search page: {str(e)}")
            # Try direct URL as fallback
            try:
                driver.get("https://kns.cnki.net/kns8/AdvSearch")
                self.human_like_delay(3, 5)
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, "gradetxt"))
                )
                self.logger.info("Advanced search page loaded via direct URL")
                return True
            except Exception as e2:
                self.logger.error(f"Failed to load direct URL too: {str(e2)}")
                return False
    
    def perform_search(self, driver, keyword):
        """
        Perform a search on CNKI
        
        Args:
            driver: Webdriver instance
            keyword: Search keyword
            
        Returns:
            int: Number of results found
        """
        try:
            # Find the search input box (there are multiple search inputs, try to find the right one)
            search_inputs = driver.find_elements(By.XPATH, "//input[contains(@class, 'ipt-txt') or @type='text']")
            
            search_input = None
            for input_elem in search_inputs:
                if input_elem.is_displayed() and input_elem.get_attribute("placeholder") != "请输入验证码":
                    search_input = input_elem
                    break
            
            if not search_input:
                self.logger.warning("Could not find a suitable search input, trying alternative method")
                # Try by explicit XPath
                search_input = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'input-box')]//input"))
                )
            
            # Clear and fill the search box
            search_input.clear()
            self.human_like_delay(0.5, 1.5)
            
            # Type the keyword character by character like a human
            for char in keyword:
                search_input.send_keys(char)
                self.human_like_delay(0.05, 0.15)
            
            self.human_like_delay()
            
            # Find and click the search button
            search_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn-search') or contains(text(), '检索')]"))
            )
            search_button.click()
            
            self.logger.info("Search initiated, waiting for results...")
            
            # Wait for search results to load
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='result-table-list'] | //div[@id='gridTable']"))
            )
            self.human_like_delay(3, 5)  # Allow results to fully render
            
            # Get the total number of results
            try:
                result_count_elem = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//span[contains(@class, 'pager_count')]/em | //span[contains(text(), '共找到')]"))
                )
                
                result_text = result_count_elem.text.strip()
                # Extract number using regex (handles commas and different formats)
                matches = re.findall(r'[\d,]+', result_text)
                if matches:
                    result_count = int(matches[0].replace(',', ''))
                    self.logger.info(f"Found {result_count} results")
                    return result_count
                else:
                    self.logger.warning(f"Could not parse result count from: {result_text}")
                    return 100  # Default value
                    
            except Exception as e:
                self.logger.warning(f"Could not determine exact result count: {str(e)}")
                # Try alternative method
                try:
                    page_info = driver.find_element(By.XPATH, "//div[contains(@class, 'search-page-con')]").text
                    matches = re.findall(r'[\d,]+', page_info)
                    if matches:
                        result_count = int(matches[0].replace(',', ''))
                        self.logger.info(f"Found {result_count} results (alternative method)")
                        return result_count
                except:
                    self.logger.warning("Using default result count")
                    return 100  # Default value
                    
        except Exception as e:
            self.logger.error(f"Error during search: {str(e)}")
            return 0
    
    def extract_article_data(self, driver, article_row, index):
        """
        Extract data from a search result row
        
        Args:
            driver: Webdriver instance
            article_row: The row element containing article data
            index: Row index for reference
            
        Returns:
            dict: Article basic data
        """
        try:
            # Find the title link
            title_link = article_row.find_element(By.XPATH, ".//a[contains(@class, 'fz14') or contains(@class, 'title')]")
            title = title_link.text.strip()
            
            # Get authors
            try:
                authors = article_row.find_element(By.XPATH, ".//td[3] | .//td[contains(@class, 'author')]").text.strip()
            except:
                authors = "无"
            
            # Get source
            try:
                source = article_row.find_element(By.XPATH, ".//td[4] | .//td[contains(@class, 'source')]").text.strip()
            except:
                source = "无"
            
            # Get date
            try:
                date = article_row.find_element(By.XPATH, ".//td[5] | .//td[contains(@class, 'date')]").text.strip()
            except:
                date = "无"
            
            # Get database
            try:
                database = article_row.find_element(By.XPATH, ".//td[6] | .//td[contains(@class, 'database')]").text.strip()
            except:
                database = "无"
            
            # Get citations
            try:
                quote = article_row.find_element(By.XPATH, ".//td[7] | .//td[contains(@class, 'quote')]").text.strip()
                if not quote.isdigit():
                    quote = "0"
            except:
                quote = "0"
            
            # Get downloads
            try:
                download = article_row.find_element(By.XPATH, ".//td[8] | .//td[contains(@class, 'download')]").text.strip()
                if not download.isdigit():
                    download = "0"
            except:
                download = "0"
            
            self.logger.info(f"Basic info for article {index}: {title}")
            
            return {
                "title": title,
                "title_link": title_link,
                "authors": authors,
                "source": source,
                "date": date,
                "database": database,
                "quote": quote,
                "download": download
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting basic article data for row {index}: {str(e)}")
            return None
    
    def get_article_details(self, driver, basic_data, index):
        """
        Get detailed information about an article by opening its page
        
        Args:
            driver: Webdriver instance
            basic_data: Basic data including the title link
            index: Article index
            
        Returns:
            dict: Complete article data
        """
        original_window = driver.current_window_handle
        
        try:
            # Click the title link to open the article page
            title_link = basic_data["title_link"]
            title_link.click()
            self.human_like_delay(3, 5)  # Wait for new window/tab to open
            
            # Switch to the new window/tab
            for window_handle in driver.window_handles:
                if window_handle != original_window:
                    driver.switch_to.window(window_handle)
                    break
            
            # Wait for the article page to load
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'doc-top') or contains(@class, 'literature-top')]"))
            )
            self.human_like_delay(2, 4)
            
            # Get the article URL
            article_url = driver.current_url
            
            # Get detailed information
            # Author institution
            try:
                # Try to click "more" button if it exists
                try:
                    more_buttons = driver.find_elements(By.XPATH, "//a[contains(@id, 'ChDivSummaryMore') or contains(@class, 'text-more')]")
                    for button in more_buttons:
                        if button.is_displayed():
                            button.click()
                            self.human_like_delay()
                except:
                    pass
                
                institute = driver.find_element(By.XPATH, "//h3[contains(text(), '作者')]/following-sibling::div[1] | //div[contains(@class, 'author')]").text.strip()
            except:
                institute = '无'
            
            # Abstract
            try:
                abstract = driver.find_element(By.XPATH, "//div[contains(@class, 'abstract-text') or @id='ChDivSummary']").text.strip()
            except:
                abstract = '无'
            
            # Keywords
            try:
                keywords = driver.find_element(By.XPATH, "//div[contains(@class, 'keywords') or @id='ChDivKeyWord']").text.strip()
            except:
                keywords = '无'
            
            # Publication
            try:
                publication_elements = driver.find_elements(By.XPATH, "//li/span[contains(text(), '专辑')]/following-sibling::p")
                publication = publication_elements[0].text.strip() if publication_elements else '无'
            except:
                publication = '无'
            
            # Topic
            try:
                topic_elements = driver.find_elements(By.XPATH, "//li/span[contains(text(), '专题')]/following-sibling::p")
                topic = topic_elements[0].text.strip() if topic_elements else '无'
            except:
                topic = '无'
            
            # Compile all data
            article_data = {
                "id": index,
                "title": basic_data["title"],
                "authors": basic_data["authors"],
                "institute": institute,
                "date": basic_data["date"],
                "source": basic_data["source"],
                "publication": publication,
                "topic": topic,
                "database": basic_data["database"],
                "quote": basic_data["quote"],
                "download": basic_data["download"],
                "keywords": keywords,
                "abstract": abstract,
                "url": article_url
            }
            
            self.logger.info(f"Successfully retrieved details for article {index}")
            
            return article_data
            
        except Exception as e:
            self.logger.error(f"Error getting article details for {index}: {str(e)}")
            # Return basic data with default values for missing fields
            return {
                "id": index,
                "title": basic_data.get("title", "无"),
                "authors": basic_data.get("authors", "无"),
                "institute": "无",
                "date": basic_data.get("date", "无"),
                "source": basic_data.get("source", "无"),
                "publication": "无",
                "topic": "无",
                "database": basic_data.get("database", "无"),
                "quote": basic_data.get("quote", "0"),
                "download": basic_data.get("download", "0"),
                "keywords": "无",
                "abstract": "无",
                "url": driver.current_url
            }
        finally:
            # Close the detail tab and switch back to results
            try:
                driver.close()
                driver.switch_to.window(original_window)
                self.human_like_delay(1, 2)
            except Exception as e:
                self.logger.error(f"Error switching back to results page: {str(e)}")
                # Try to recover
                if len(driver.window_handles) > 0:
                    driver.switch_to.window(driver.window_handles[0])
    
    def go_to_next_page(self, driver):
        """
        Navigate to the next page of search results
        
        Args:
            driver: Webdriver instance
            
        Returns:
            bool: Whether navigation was successful
        """
        try:
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//a[@id='PageNext' or contains(@class, 'next') or contains(text(), '下一页')]"))
            )
            
            # Scroll to the button to ensure it's visible
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
            self.human_like_delay()
            
            next_button.click()
            self.human_like_delay(2, 4)
            
            # Wait for the new page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='result-table-list'] | //div[@id='gridTable']"))
            )
            self.human_like_delay(2, 3)  # Additional wait for stability
            
            return True
        except Exception as e:
            self.logger.error(f"Error navigating to next page: {str(e)}")
            return False
    
    def crawl_articles(self, driver, max_results):
        """
        Crawl articles from CNKI search results
        
        Args:
            driver: Webdriver instance
            max_results: Maximum number of results to collect
            
        Returns:
            list: Crawled articles data
        """
        articles = []
        current_index = 1
        
        try:
            while current_index <= max_results:
                # Find all article rows on the current page
                article_rows = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//table[contains(@class, 'result-table-list')]/tbody/tr"))
                )
                
                # Calculate how many articles to process from this page
                remaining = max_results - (current_index - 1)
                items_to_process = min(len(article_rows), remaining)
                
                self.logger.info(f"Found {len(article_rows)} articles on current page, processing {items_to_process}")
                
                # Process each article row
                for i in range(items_to_process):
                    try:
                        self.logger.info(f"\n### Processing article {current_index} (Page {(current_index-1)//20 + 1}, Item {i+1}) ###")
                        
                        # Get basic data from the search results page
                        basic_data = self.extract_article_data(driver, article_rows[i], current_index)
                        
                        if basic_data:
                            # Get detailed data by opening the article page
                            article_data = self.get_article_details(driver, basic_data, current_index)
                            
                            if article_data:
                                articles.append(article_data)
                                
                                # Format and write to TSV file
                                self.write_article_to_file(article_data)
                        
                        # Increment counter
                        current_index += 1
                        
                        # Refresh the page content if this is not the last item
                        if i < items_to_process - 1:
                            # Refetch the article rows as the DOM might have changed
                            article_rows = WebDriverWait(driver, 10).until(
                                EC.presence_of_all_elements_located((By.XPATH, "//table[contains(@class, 'result-table-list')]/tbody/tr"))
                            )
                    
                    except Exception as e:
                        self.logger.error(f"Error processing article {current_index}: {str(e)}")
                        current_index += 1  # Continue with next article
                
                # Check if we need to go to the next page
                if current_index <= max_results:
                    if not self.go_to_next_page(driver):
                        self.logger.warning("Could not navigate to next page, stopping crawl")
                        break
                else:
                    break
                    
        except Exception as e:
            self.logger.error(f"Error during article crawling: {str(e)}")
        
        self.logger.info(f"Crawling completed. Collected {len(articles)} articles.")
        return articles
    
    def write_article_to_file(self, article_data, output_file=None):
        """
        Write article data to TSV file
        
        Args:
            article_data: Article data dictionary
            output_file: Output file path (optional)
        """
        if output_file is None:
            output_file = os.path.join(self.output_dir, f"cnki_articles_{datetime.now().strftime('%Y%m%d')}.tsv")
        
        try:
            # Format the data as a TSV line
            fields = [
                str(article_data.get("id", "")),
                article_data.get("title", ""),
                article_data.get("authors", ""),
                article_data.get("institute", ""),
                article_data.get("date", ""),
                article_data.get("source", ""),
                article_data.get("publication", ""),
                article_data.get("topic", ""),
                article_data.get("database", ""),
                article_data.get("quote", ""),
                article_data.get("download", ""),
                article_data.get("keywords", ""),
                article_data.get("abstract", ""),
                article_data.get("url", "")
            ]
            
            # Replace newlines and tabs to prevent breaking the TSV format
            fields = [field.replace("\n", " ").replace("\t", " ") for field in fields]
            
            line = "\t".join(fields) + "\n"
            
            # Check if the file exists to decide whether to write headers
            write_header = not os.path.exists(output_file) or os.path.getsize(output_file) == 0
            
            with open(output_file, 'a', encoding='utf-8') as f:
                if write_header:
                    headers = "id\ttitle\tauthors\tinstitute\tdate\tsource\tpublication\ttopic\tdatabase\tquote\tdownload\tkeywords\tabstract\turl\n"
                    f.write(headers)
                f.write(line)
            
            self.logger.info(f"Successfully wrote article {article_data['id']} to file")
            
        except Exception as e:
            self.logger.error(f"Error writing to file: {str(e)}")
    
    def save_results_as_json(self, articles, theme):
        """
        Save articles data as JSON file
        
        Args:
            articles: List of article data dictionaries
            theme: Search theme/keyword
            
        Returns:
            str: Path to the saved JSON file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = os.path.join(self.output_dir, f"cnki_results_{theme}_{timestamp}.json")
        
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Successfully saved {len(articles)} articles to {json_path}")
            return json_path
            
        except Exception as e:
            self.logger.error(f"Error saving JSON file: {str(e)}")
            return None
    
    def save_results_as_csv(self, articles, theme):
        """
        Save articles data as CSV file
        
        Args:
            articles: List of article data dictionaries
            theme: Search theme/keyword
            
        Returns:
            str: Path to the saved CSV file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(self.output_dir, f"cnki_results_{theme}_{timestamp}.csv")
        
        try:
            df = pd.DataFrame(articles)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')  # Use utf-8-sig for Excel compatibility
            
            self.logger.info(f"Successfully saved {len(articles)} articles to {csv_path}")
            return csv_path
            
        except Exception as e:
            self.logger.error(f"Error saving CSV file: {str(e)}")
            return None
    
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
        
        # Create driver
        driver = self.setup_driver()
        
        try:
            # Navigate to search page
            if not self.navigate_to_search_page(driver):
                raise Exception("Failed to load search page")
            
            # Perform search
            result_count = self.perform_search(driver, term)
            
            if result_count == 0:
                self.logger.warning("No results found or error during search")
                return {"count": 0, "results": [], "error": "No results found"}
            
            # Wait for manual interaction - this is a critical step for CNKI
            input("Please check the search results and press Enter to continue...")
            
            # Determine how many papers to crawl
            papers_need = min(max_results, result_count)
            self.logger.info(f"Will crawl up to {papers_need} articles out of {result_count} total results")
            
            # Start crawling
            articles = self.crawl_articles(driver, papers_need)
            
            # Save results
            json_path = self.save_results_as_json(articles, term)
            csv_path = self.save_results_as_csv(articles, term)
            
            return {
                "count": result_count,
                "results": articles,
                "json_path": json_path,
                "csv_path": csv_path
            }
            
        except Exception as e:
            self.logger.error(f"Error during CNKI search: {str(e)}")
            import traceback
            traceback_str = traceback.format_exc()
            self.logger.error(traceback_str)
            return {"error": str(e), "traceback": traceback_str}
            
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
    parser = argparse.ArgumentParser(description="CNKI Undetected Chrome Crawler")
    parser.add_argument("term", help="Search term")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--max-results", type=int, default=100, help="Maximum number of results to collect")
    parser.add_argument("--db-code", default="CJFD", choices=["CJFD", "CDFD", "CMFD"], 
                        help="Database code: CJFD (journals), CDFD (PhD theses), CMFD (Master theses)")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode (no visible browser)")
    
    args = parser.parse_args()
    
    # Create crawler and run search
    crawler = CNKIUndetectedCrawler(output_dir=args.output_dir, headless=args.headless)
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
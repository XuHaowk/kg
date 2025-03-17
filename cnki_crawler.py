# cnki_wrapper.py
import os
import sys
import time
import json
import pandas as pd
from datetime import datetime
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.edge.service import Service

class CNKIWrapper:
    """Standalone wrapper for CNKI crawling functionality"""
    
    def __init__(self, output_dir="output"):
        """
        Initialize the CNKI crawler wrapper
        
        Args:
            output_dir (str): Output directory path
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def webserver(self):
        """Create and return an Edge browser instance"""
        # Set page load strategy to none (don't wait for page load)
        desired_capabilities = DesiredCapabilities.EDGE
        desired_capabilities["pageLoadStrategy"] = "none"

        # Set up Edge browser options
        options = webdriver.EdgeOptions()
        # Disable loading images to improve speed
        options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
        
        # Create Edge driver
        driver = webdriver.Edge(options=options)
        return driver
    
    def get_info(self, driver, xpath):
        """Get text from element by xpath with error handling"""
        try:
            element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
            return element.text
        except:
            return '无'

    def get_choose_info(self, driver, xpath1, xpath2, search_str):
        """Get specific information by checking condition first"""
        try:
            if WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, xpath1))).text == search_str:
                return WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, xpath2))).text
            else:
                return '无'
        except:
            return '无'
    
    def search_cnki(self, term, date_range=None, max_results=100, db_code="CJFD"):
        """
        Search CNKI literature using a standalone implementation
        
        Args:
            term (str): Search term
            date_range (tuple): Date range in format (start_date, end_date)
            max_results (int): Maximum number of results to collect
            db_code (str): Database code (not used in current implementation)
            
        Returns:
            dict: Dictionary containing search results
        """
        try:
            print(f"Starting CNKI search for '{term}'")
            
            # Setup the Edge browser driver
            driver = self.webserver()
            
            # Open the search page
            driver.get("https://kns.cnki.net/kns8/AdvSearch")
            time.sleep(2)
            
            # Modify attributes to display dropdown
            opt = driver.find_element(By.CSS_SELECTOR, 'div.sort-list')
            driver.execute_script("arguments[0].setAttribute('style', 'display: block;')", opt)

            # Move mouse to dropdown [Correspondent Author]
            ActionChains(driver).move_to_element(driver.find_element(By.CSS_SELECTOR, 'li[data-val="RP"]')).perform()
            
            # Input keyword
            keyword_input = WebDriverWait(driver, 100).until(
                EC.presence_of_element_located((By.XPATH, '''//*[@id="gradetxt"]/dd[1]/div[2]/input'''))
            )
            keyword_input.send_keys(term)
            
            # Apply date range if specified
            if date_range and len(date_range) == 2 and date_range[0] and date_range[1]:
                try:
                    # Find date inputs
                    time_inputs = driver.find_elements(By.CSS_SELECTOR, '.input-time .year-input')
                    if len(time_inputs) >= 2:
                        start_date_input = time_inputs[0]
                        end_date_input = time_inputs[1]
                        
                        # Format dates for CNKI input format (YYYY-MM-DD)
                        start_date = date_range[0].replace('/', '-')
                        end_date = date_range[1].replace('/', '-')
                        
                        # Clear and fill inputs
                        start_date_input.clear()
                        start_date_input.send_keys(start_date)
                        end_date_input.clear()
                        end_date_input.send_keys(end_date)
                        
                        print(f"Applied date filter: {start_date} to {end_date}")
                    else:
                        print("Date range inputs not found")
                except Exception as e:
                    print(f"Error applying date range: {str(e)}")
            
            # Click search button
            search_button = WebDriverWait(driver, 100).until(
                EC.presence_of_element_located(
                    (By.XPATH, '''//*[@id="ModuleSearch"]/div[1]/div/div[2]/div/div[1]/div[1]/div[2]/div[3]/input'''))
            )
            search_button.click()
            
            print("Searching, please wait...")
            
            # Wait for results to load
            time.sleep(5)
            
            # Get total number of results
            res_element = WebDriverWait(driver, 100).until(EC.presence_of_element_located(
                (By.XPATH, '''//*[@id="countPageDiv"]/span[1]/em'''))
            )
            
            # Remove commas from thousands
            res_count = int(res_element.text.replace(",", ''))
            page_count = int(res_count / 20) + 1
            print(f"Found {res_count} results, {page_count} pages.")
            
            # Determine how many papers to process
            papers_need = min(max_results, res_count)
            print(f"Will process up to {papers_need} articles")
            
            # Wait for user to check search results
            input("Please check the search results and press Enter to continue...")
            
            # Generate file path for results
            file_path = os.path.join(self.output_dir, f"{term}.tsv")
            
            # Start crawling articles with our implementation
            articles = self.crawl_articles(driver, papers_need, term, file_path)
            
            # Save as JSON
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            
            # Also save as CSV for better compatibility
            csv_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.csv")
            pd.DataFrame(articles).to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            # Close the browser
            driver.close()
            
            return {
                "count": res_count,
                "results": articles,
                "json_path": json_path,
                "csv_path": csv_path
            }
            
        except Exception as e:
            import traceback
            print(f"Error during CNKI search: {str(e)}")
            print(traceback.format_exc())
            
            # Try to close the browser if it exists
            try:
                driver.close()
            except:
                pass
                
            return {"error": str(e)}
    
    def crawl_articles(self, driver, papers_need, theme, output_file=None):
        """
        Standalone version of the crawl function that accepts a custom output path
        
        Args:
            driver: Selenium WebDriver instance
            papers_need: Number of papers to crawl
            theme: Search theme/keyword
            output_file: Custom output file path
            
        Returns:
            list: List of article data dictionaries
        """
        count = 1
        articles = []

        # Create or use the provided output file path
        if output_file is None:
            file_path = os.path.join(self.output_dir, f"{theme}.tsv")
        else:
            file_path = output_file
            
        # Ensure the directory exists
        file_dir = os.path.dirname(file_path)
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        
        # Check if file exists to decide whether to continue or start fresh
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            with open(file_path, "r", encoding='gbk', errors='ignore') as file:
                lines = file.readlines()
                if lines:
                    last_line = lines[-1].strip()
                    try:
                        count = int(last_line.split("\t")[0]) + 1
                    except (ValueError, IndexError):
                        count = 1
                        print("Could not get count from previous file, starting from 1")
        
        # Skip to correct page
        current_page = 1
        target_page = ((count - 1) // 20) + 1
        
        while current_page < target_page:
            # Switch to next page
            time.sleep(3)
            try:
                next_button = driver.find_element(By.XPATH, "//*[@id='PageNext']")
                next_button.click()
                print(f"Skipped to page {current_page+1}")
                current_page += 1
            except Exception as e:
                print(f"Cannot skip to page {current_page+1}: {str(e)}")
                break

        print(f"Starting from item {count}\n")

        # When crawled count is less than needed, loop through web pages
        while count <= papers_need:
            # Wait for page to load completely
            time.sleep(3)

            try:
                # Find all titles on the page
                title_list = driver.find_elements(By.CLASS_NAME, "fz14")
                
                # Loop through items on current page
                start_item = (count - 1) % 20 + 1
                end_item = min(len(title_list) + 1, 21)  # +1 because of 1-indexing
                
                for i in range(start_item, end_item):
                    if count > papers_need:
                        break

                    print(f"\n###Crawling item {count} (Page {(count - 1) // 20 + 1}, Item {i})#######################################\n")

                    try:
                        term = (count - 1) % 20 + 1  # Item number on this page

                        # Get basic info
                        print('Getting basic info...')
                        title_xpath = f'''//*[@id="gridTable"]/div/div/table/tbody/tr[{term}]/td[2]'''
                        author_xpath = f'''//*[@id="gridTable"]/div/div/table/tbody/tr[{term}]/td[3]'''
                        source_xpath = f'''//*[@id="gridTable"]/div/div/table/tbody/tr[{term}]/td[4]'''
                        date_xpath = f'''//*[@id="gridTable"]/div/div/table/tbody/tr[{term}]/td[5]'''
                        database_xpath = f'''//*[@id="gridTable"]/div/div/table/tbody/tr[{term}]/td[6]'''
                        quote_xpath = f'''//*[@id="gridTable"]/div/div/table/tbody/tr[{term}]/td[7]'''
                        download_xpath = f'''//*[@id="gridTable"]/div/div/table/tbody/tr[{term}]/td[8]'''
                        
                        # Get text from elements using our own implementation
                        xpaths = [title_xpath, author_xpath, source_xpath, date_xpath, database_xpath, quote_xpath, download_xpath]
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future_elements = [executor.submit(self.get_info, driver, xpath) for xpath in xpaths]
                        title, authors, source, date, database, quote, download = [future.result() for future in future_elements]
                        
                        if not quote.isdigit():
                            quote = '0'
                        if not download.isdigit():
                            download = '0'
                        print(f"{title} {authors} {source} {date} {database} {quote} {download}\n")

                        # Click on the item
                        title_list[i - 1].click()

                        # Get driver handles
                        n = driver.window_handles

                        # Switch to the newly opened page
                        driver.switch_to.window(n[-1])
                        time.sleep(3)

                        # Get page information
                        # Click expand if necessary
                        try:
                            expand_button = driver.find_element(By.XPATH, '''//*[@id="ChDivSummaryMore"]''')
                            expand_button.click()
                        except:
                            pass

                        # Get author affiliation
                        print('Getting institute...')
                        try:
                            institute = driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div[3]/div/div/div[3]/div/h3[2]").text
                        except:
                            institute = '无'
                        print(institute + '\n')

                        # Get abstract
                        print('Getting abstract...')
                        try:
                            abstract = driver.find_element(By.CLASS_NAME, "abstract-text").text
                        except:
                            abstract = '无'
                        print(abstract + '\n')

                        # Get keywords
                        print('Getting keywords...')
                        try:
                            keywords = driver.find_element(By.CLASS_NAME, "keywords").text[:-1]
                        except:
                            keywords = '无'
                        print(keywords + '\n')

                        # Get publication
                        print('Getting publication...')
                        publication_xpaths = [
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[1]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[1]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[2]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[2]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[1]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[1]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[2]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[2]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[4]/ul/li[1]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[4]/ul/li[1]/p")
                        ]
                        
                        publication_results = []
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            futures = [executor.submit(self.get_choose_info, driver, xpath1, xpath2, '专辑：') 
                                      for xpath1, xpath2 in publication_xpaths]
                            publication_results = [future.result() for future in concurrent.futures.as_completed(futures)]
                        
                        publication = next((result for result in publication_results if result != '无'), '无')
                        print(publication + '\n')

                        # Get topic info
                        print('Getting topic...')
                        topic_xpaths = [
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[2]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[2]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[3]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[6]/ul/li[3]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[2]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[2]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[3]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[7]/ul/li[3]/p"),
                            ("/html/body/div[2]/div[1]/div[3]/div/div/div[4]/ul/li[2]/span",
                             "/html/body/div[2]/div[1]/div[3]/div/div/div[4]/ul/li[2]/p")
                        ]
                        
                        topic_results = []
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            futures = [executor.submit(self.get_choose_info, driver, xpath1, xpath2, '专题：') 
                                      for xpath1, xpath2 in topic_xpaths]
                            topic_results = [future.result() for future in concurrent.futures.as_completed(futures)]
                        
                        topic = next((result for result in topic_results if result != '无'), '无')
                        print(topic + '\n')

                        # Get current URL
                        url = driver.current_url

                        # Create article data
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
                        
                        # Add to articles list
                        articles.append(article_data)

                        # Format and write to TSV
                        res = f"{count}\t{title}\t{authors}\t{institute}\t{date}\t{source}\t{publication}\t{topic}\t{database}\t{quote}\t{download}\t{keywords}\t{abstract}\t{url}".replace("\n", "") + "\n"

                        try:
                            with open(file_path, 'a', encoding='gbk') as f:
                                f.write(res)
                                print('Write successful')
                        except Exception as e:
                            # Try with utf-8 if gbk fails
                            try:
                                with open(file_path, 'a', encoding='utf-8') as f:
                                    f.write(res)
                                    print('Write successful with utf-8 encoding')
                            except Exception as utf_e:
                                print(f'Write failed: {str(utf_e)}')
                    except Exception as e:
                        print(f" Item {count} crawling failed: {str(e)}")
                        # Skip this item and continue to next one
                        

                    finally:
                        # If multiple windows are open, close the detail page and switch back to results
                        n2 = driver.window_handles
                        if len(n2) > 1:
                            driver.close()
                            driver.switch_to.window(n2[0])
                        # Increment count and check if we have enough
                        count += 1
                        if count > papers_need: 
                            break

                # Move to next page if needed
                if count <= papers_need:
                    try:
                        next_button = driver.find_element(By.XPATH, "//a[@id='PageNext']")
                        next_button.click()
                        time.sleep(2)
                    except Exception as e:
                        print(f"No more pages or error going to next page: {str(e)}")
                        break
            except Exception as e:
                print(f"Error processing page: {str(e)}")
                break

        print("Crawling complete!")
        return articles
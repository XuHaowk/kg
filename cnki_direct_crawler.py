#!/usr/bin/env python3
"""
CNKI Direct HTTP Crawler

This module provides a direct HTTP-based crawler for the CNKI (China National Knowledge
Infrastructure) website without using browser automation.
"""

import os
import re
import sys
import time
import json
import random
import logging
import urllib.parse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd

class CNKIDirectCrawler:
    """CNKI crawler using direct HTTP requests"""
    
    def __init__(self, output_dir="output"):
        """
        Initialize the CNKI Direct crawler
        
        Args:
            output_dir (str): Output directory path
        """
        self.output_dir = output_dir
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Configure logging
        self.logger = self._setup_logger()
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
    
    def _setup_logger(self):
        """Set up logger"""
        logger = logging.getLogger("CNKIDirectCrawler")
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
    
    def human_delay(self, min_sec=1, max_sec=3):
        """Add random delay to simulate human behavior"""
        time.sleep(random.uniform(min_sec, max_sec))
    
    def get_search_page(self):
        """
        Get the search page HTML and extract necessary parameters
        
        Returns:
            dict: Search parameters
        """
        try:
            # First access the CNKI homepage
            self.logger.info("Accessing CNKI homepage")
            response = self.session.get("https://www.cnki.net/", timeout=30)
            response.raise_for_status()
            
            # Now access the advanced search page
            self.logger.info("Accessing advanced search page")
            response = self.session.get("https://kns.cnki.net/kns8/AdvSearch", timeout=30)
            response.raise_for_status()
            
            # Parse the HTML to extract session parameters
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract form data
            params = {}
            for input_tag in soup.find_all('input', attrs={'type': 'hidden'}):
                if input_tag.get('name') and input_tag.get('value'):
                    params[input_tag['name']] = input_tag['value']
            
            self.logger.info(f"Successfully loaded search page with {len(params)} parameters")
            return params
            
        except Exception as e:
            self.logger.error(f"Error retrieving search page: {str(e)}")
            raise
    
    def perform_search(self, term, db_code="CJFD"):
        """
        Perform search on CNKI
        
        Args:
            term (str): Search term
            db_code (str): Database code
            
        Returns:
            tuple: (response HTML, result count)
        """
        try:
            self.logger.info(f"Performing search for term: {term}")
            
            # Get search parameters from the search page
            search_params = self.get_search_page()
            
            # Prepare search data
            search_data = {
                'searchType': 'MulityTermsSearch',
                'ArticleType': '',  # Leave empty for all article types
                'SearchKeyList': term,
                'ParamIsNullOrEmpty': 'true',
                'Islegal': 'false',
                'DbPrefix': 'SCDB',
                'DbText': db_code,  # Set appropriate database
                'TrueFileList': '',
                'IsRecall': 'false',
                'Subject': '',
                'Research': '',  # No specific research field
                'ImportMode': 'General',
                'pageindex': '',
                'QueryID': '',
                'turnpage': '',
                'RecordShowOrder': '0',
                'RecordShowLang': ''
            }
            
            # Update with parameters from the search page
            search_data.update(search_params)
            
            # Set headers for search request
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://kns.cnki.net',
                'Referer': 'https://kns.cnki.net/kns8/AdvSearch',
            }
            
            # Perform the search POST request
            search_url = "https://kns.cnki.net/kns8/Brief/ShortSearch"
            response = self.session.post(
                search_url,
                data=search_data,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            # Add a delay to seem more human-like
            self.human_delay(2, 4)
            
            # Get the search results
            results_url = "https://kns.cnki.net/kns8/Brief/GetGridTableHtml"
            results_response = self.session.get(results_url, timeout=30)
            results_response.raise_for_status()
            
            # Parse the HTML to get the result count
            soup = BeautifulSoup(results_response.text, 'html.parser')
            
            # Try to find the result count
            count_elem = soup.select_one('.pager_container .pager_count')
            result_count = 0
            
            if count_elem:
                count_text = count_elem.get_text(strip=True)
                match = re.search(r'(\d+(?:,\d+)*)', count_text)
                if match:
                    result_count = int(match.group(1).replace(',', ''))
            
            # If we couldn't find the count in the expected place, try an alternative
            if result_count == 0:
                count_text = soup.get_text()
                match = re.search(r'共(\d+(?:,\d+)*)条', count_text)
                if match:
                    result_count = int(match.group(1).replace(',', ''))
            
            self.logger.info(f"Found {result_count} results")
            return results_response.text, result_count
            
        except Exception as e:
            self.logger.error(f"Error performing search: {str(e)}")
            raise
    
    def parse_search_results(self, html, max_pages=50):
        """
        Parse search results HTML to extract article URLs
        
        Args:
            html (str): HTML content of search results
            max_pages (int): Maximum number of pages to crawl
            
        Returns:
            list: List of article URLs
        """
        article_urls = []
        current_page = 1
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Process the first page
            self.logger.info("Processing search results page 1")
            
            # Find all article links on the current page
            for link in soup.select('.fz14'):
                if link.has_attr('href') and "/kns8/Detail?" in link['href']:
                    article_url = "https://kns.cnki.net" + link['href']
                    article_urls.append(article_url)
            
            self.logger.info(f"Found {len(article_urls)} articles on page 1")
            
            # Get the pagination information
            page_info = soup.select_one('.countPageMark')
            if page_info:
                total_pages_match = re.search(r'/(\d+)', page_info.get_text(strip=True))
                if total_pages_match:
                    total_pages = int(total_pages_match.group(1))
                    total_pages = min(total_pages, max_pages)
                    
                    self.logger.info(f"Total pages: {total_pages}")
                    
                    # Process subsequent pages
                    for page in range(2, total_pages + 1):
                        self.logger.info(f"Processing search results page {page}")
                        
                        # Add a delay between page requests
                        self.human_delay(2, 5)
                        
                        # Request the next page
                        next_page_url = f"https://kns.cnki.net/kns8/Brief/GetGridTableHtml?pageindex={page}"
                        response = self.session.get(next_page_url, timeout=30)
                        response.raise_for_status()
                        
                        # Parse the new page
                        page_soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Find all article links on this page
                        page_articles = []
                        for link in page_soup.select('.fz14'):
                            if link.has_attr('href') and "/kns8/Detail?" in link['href']:
                                article_url = "https://kns.cnki.net" + link['href']
                                page_articles.append(article_url)
                                article_urls.append(article_url)
                        
                        self.logger.info(f"Found {len(page_articles)} articles on page {page}")
            
            return article_urls
            
        except Exception as e:
            self.logger.error(f"Error parsing search results: {str(e)}")
            return article_urls  # Return whatever we've collected so far
    
    def extract_article_details(self, url):
        """
        Extract article details from article page
        
        Args:
            url (str): Article URL
            
        Returns:
            dict: Article details
        """
        try:
            self.logger.info(f"Extracting details from {url}")
            
            # Add a delay to seem more human-like
            self.human_delay(2, 4)
            
            # Request the article page
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse the HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title = "无"
            title_elem = soup.select_one('h1')
            if title_elem:
                title = title_elem.get_text(strip=True)
            
            # Extract authors
            authors = "无"
            authors_elem = soup.select_one('.author')
            if authors_elem:
                authors = authors_elem.get_text(strip=True)
            
            # Extract institute/affiliation
            institute = "无"
            institute_elem = soup.select_one('.orgn')
            if institute_elem:
                institute = institute_elem.get_text(strip=True)
            
            # Extract date
            date = "无"
            date_elem = soup.select_one('.date')
            if date_elem:
                date = date_elem.get_text(strip=True)
            
            # Extract source
            source = "无"
            source_elem = soup.select_one('.top-tip a')
            if source_elem:
                source = source_elem.get_text(strip=True)
            
            # Extract abstract
            abstract = "无"
            abstract_elem = soup.select_one('#ChDivSummary')
            if abstract_elem:
                abstract = abstract_elem.get_text(strip=True)
            
            # Extract keywords
            keywords = "无"
            keywords_elem = soup.select('.keywords a')
            if keywords_elem:
                keywords = '; '.join([k.get_text(strip=True) for k in keywords_elem])
            
            # Extract quotes (citations)
            quote = "0"
            quote_elem = soup.select_one('.quote-count')
            if quote_elem:
                quote_text = quote_elem.get_text(strip=True)
                quote_match = re.search(r'\d+', quote_text)
                if quote_match:
                    quote = quote_match.group(0)
            
            # Extract downloads
            download = "0"
            download_elem = soup.select_one('.download-count')
            if download_elem:
                download_text = download_elem.get_text(strip=True)
                download_match = re.search(r'\d+', download_text)
                if download_match:
                    download = download_match.group(0)
            
            # Extract publication info
            publication = "无"
            topic = "无"
            database = "无"
            
            for info_elem in soup.select('.top-space'):
                label = info_elem.select_one('label')
                if label:
                    label_text = label.get_text(strip=True)
                    if "专辑：" in label_text:
                        p_elem = info_elem.select_one('p')
                        if p_elem:
                            publication = p_elem.get_text(strip=True)
                    elif "专题：" in label_text:
                        p_elem = info_elem.select_one('p')
                        if p_elem:
                            topic = p_elem.get_text(strip=True)
                    elif "数据库：" in label_text:
                        p_elem = info_elem.select_one('p')
                        if p_elem:
                            database = p_elem.get_text(strip=True)
            
            # Compile the article data
            article_data = {
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
            
            self.logger.info(f"Successfully extracted details for article: {title}")
            return article_data
            
        except Exception as e:
            self.logger.error(f"Error extracting article details: {str(e)}")
            # Return a default article data structure with the URL
            return {
                "title": "无 (获取失败)",
                "authors": "无",
                "institute": "无",
                "date": "无",
                "source": "无",
                "publication": "无",
                "topic": "无",
                "database": "无",
                "quote": "0",
                "download": "0",
                "keywords": "无",
                "abstract": "无",
                "url": url,
                "error": str(e)
            }
    
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
        
        try:
            # Perform the search
            search_html, result_count = self.perform_search(term, db_code)
            
            if result_count == 0:
                self.logger.warning("No results found")
                return {"count": 0, "results": []}
            
            # Wait for manual confirmation
            input("Please check the search results in the console and press Enter to continue...")
            
            # Parse search results to get article URLs
            max_pages = (max_results + 19) // 20  # Calculate max pages needed to get max_results
            article_urls = self.parse_search_results(search_html, max_pages)
            
            # Limit number of articles to max_results
            article_urls = article_urls[:max_results]
            self.logger.info(f"Will process {len(article_urls)} articles")
            
            # Extract details for each article
            articles = []
            for i, url in enumerate(article_urls, 1):
                self.logger.info(f"Processing article {i}/{len(article_urls)}")
                article_data = self.extract_article_details(url)
                article_data["id"] = i  # Add ID
                articles.append(article_data)
                
                # Write to TSV file
                self.write_article_to_file(article_data, i, term)
                
                # Add a delay between requests
                self.human_delay(1, 3)
            
            # Save results as JSON and CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            
            csv_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.csv")
            df = pd.DataFrame(articles)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"Search completed. Crawled {len(articles)} articles out of {result_count} results.")
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
    
    def write_article_to_file(self, article_data, index, theme):
        """
        Write article data to TSV file
        
        Args:
            article_data (dict): Article data
            index (int): Article index
            theme (str): Search theme/keyword
        """
        try:
            output_file = os.path.join(self.output_dir, f"{theme}_{datetime.now().strftime('%Y%m%d')}.tsv")
            
            # Check if file exists to write headers
            write_header = not os.path.exists(output_file) or os.path.getsize(output_file) == 0
            
            # Format the data as a TSV line
            fields = [
                str(index),
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
            fields = [str(field).replace("\n", " ").replace("\t", " ") for field in fields]
            
            line = "\t".join(fields) + "\n"
            
            with open(output_file, 'a', encoding='utf-8') as f:
                if write_header:
                    headers = "id\ttitle\tauthors\tinstitute\tdate\tsource\tpublication\ttopic\tdatabase\tquote\tdownload\tkeywords\tabstract\turl\n"
                    f.write(headers)
                f.write(line)
            
            self.logger.info(f"Successfully wrote article {index} to file")
            
        except Exception as e:
            self.logger.error(f"Error writing to file: {str(e)}")


# If run as script
if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="CNKI Direct HTTP Crawler")
    parser.add_argument("term", help="Search term")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument("--max-results", type=int, default=100, help="Maximum number of results to collect")
    parser.add_argument("--db-code", default="CJFD", choices=["CJFD", "CDFD", "CMFD"], 
                       help="Database code: CJFD (journals), CDFD (PhD theses), CMFD (Master theses)")
    
    args = parser.parse_args()
    
    # Create crawler and run search
    crawler = CNKIDirectCrawler(output_dir=args.output_dir)
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
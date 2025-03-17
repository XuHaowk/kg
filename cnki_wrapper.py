# cnki_wrapper.py
import os
import sys
import time
import pandas as pd
from datetime import datetime

# Import the functions from cnki.py
from cnki import webserver, open_page, crawl

class CNKIWrapper:
    """Wrapper for the CNKI crawler functionality in cnki.py"""
    
    def __init__(self, output_dir="output"):
        """
        Initialize the CNKI crawler wrapper
        
        Args:
            output_dir (str): Output directory path
        """
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def search_cnki(self, term, date_range=None, max_results=100, db_code="CJFD"):
        """
        Search CNKI literature using the existing crawler
        
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
            driver = webserver()
            
            # Open the search page and get result count
            res_count = open_page(driver, term)
            
            # Determine how many papers to process
            papers_need = min(max_results, res_count)
            print(f"Found {res_count} results, will process up to {papers_need}")
            
            # Wait for user to check search results
            input("Please check the search results and press Enter to continue...")
            
            # Start crawling articles
            file_path = os.path.join(self.output_dir, f"{term}.tsv")
            crawl(driver, papers_need, term, file_path)
            
            # Convert TSV to JSON for consistency with other outputs
            articles = self._convert_tsv_to_json(file_path)
            
            # Save as JSON
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_path = os.path.join(self.output_dir, f"cnki_results_{timestamp}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                import json
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
            return {"error": str(e)}
    
    def _convert_tsv_to_json(self, tsv_file):
        """Convert TSV output to JSON format"""
        try:
            # Read TSV file
            df = pd.read_csv(tsv_file, sep='\t', encoding='utf-8')
            
            # Convert DataFrame to list of dictionaries
            articles = []
            for _, row in df.iterrows():
                article = {
                    "title": row.get('title', ''),
                    "authors": row.get('authors', ''),
                    "institute": row.get('institute', ''),
                    "date": row.get('date', ''),
                    "source": row.get('source', ''),
                    "publication": row.get('publication', ''),
                    "topic": row.get('topic', ''),
                    "database": row.get('database', ''),
                    "quote": row.get('quote', '0'),
                    "download": row.get('download', '0'),
                    "keywords": row.get('keywords', ''),
                    "abstract": row.get('abstract', ''),
                    "url": row.get('url', '')
                }
                articles.append(article)
            
            return articles
            
        except Exception as e:
            print(f"Error converting TSV to JSON: {str(e)}")
            return []

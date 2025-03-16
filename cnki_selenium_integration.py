#!/usr/bin/env python3
"""
CNKI Selenium Integration Module

This module provides integration between the KG application and the Selenium-based CNKI web scraper.
"""

import logging
import threading
from cnki_selenium_fixed import CNKIWebScraper

def is_selenium_available():
    """Check if Selenium is available"""
    try:
        import selenium
        from selenium import webdriver
        from webdriver_manager.chrome import ChromeDriverManager
        return True
    except ImportError:
        return False

def show_installation_instructions():
    """Show instructions for installing Selenium"""
    instructions = """
    请安装以下依赖以启用Selenium自动爬取功能:
    
    pip install selenium webdriver-manager pandas
    
    安装完成后，重启应用程序。
    """
    return instructions

class CNKISeleniumIntegration:
    """Integration class for Selenium-based CNKI scraper"""
    
    def __init__(self, logger=None):
        """Initialize the integration class"""
        self.logger = logger or logging.getLogger("CNKISeleniumIntegration")
        self.thread = None
        self.scraper = None
    
    def start_crawler(self, username, password, term, date_range, max_results, db_code, 
                      output_dir, headless=False, callback=None, use_manual_mode=False):
        """
        Start the CNKI crawler in a separate thread
        
        Args:
            username: CNKI username
            password: CNKI password
            term: Search term
            date_range: Date range tuple (start_date, end_date)
            max_results: Maximum number of results to collect
            db_code: Database code
            output_dir: Output directory
            headless: Whether to run in headless mode
            callback: Callback function to receive results
            use_manual_mode: Whether to use manual mode
            
        Returns:
            threading.Thread: The crawler thread
        """
        if not is_selenium_available():
            self.logger.error("Selenium is not available")
            return None
        
        # Define the crawler thread function
        def crawler_thread():
            try:
                # Create the scraper instance
                self.scraper = CNKIWebScraper(
                    username=username,
                    password=password,
                    output_dir=output_dir,
                    headless=headless,
                    debug_mode=True
                )
                
                # Execute the search
                if use_manual_mode:
                    # Use manual collection mode
                    results = self.scraper.manual_collection_mode(term, output_dir)
                else:
                    # Use automatic search
                    results = self.scraper.search_and_collect(
                        term=term,
                        date_range=date_range,
                        max_results=max_results,
                        db_code=db_code
                    )
                
                # Call the callback with results
                if callback:
                    callback(results)
                
                # Close the scraper
                self.scraper.close()
                self.scraper = None
                
            except Exception as e:
                self.logger.error(f"Error in crawler thread: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
                
                # Call callback with error
                if callback:
                    callback({"status": "error", "message": str(e), "results": []})
                
                # Ensure scraper is closed
                if self.scraper:
                    self.scraper.close()
                    self.scraper = None
        
        # Create and start the thread
        self.thread = threading.Thread(target=crawler_thread)
        self.thread.daemon = True
        self.thread.start()
        
        return self.thread
    
    def stop_crawler(self):
        """Stop the crawler if it's running"""
        if self.scraper:
            try:
                self.logger.info("Stopping crawler")
                self.scraper.close()
                self.scraper = None
            except Exception as e:
                self.logger.error(f"Error stopping crawler: {str(e)}")
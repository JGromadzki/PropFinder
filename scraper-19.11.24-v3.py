import streamlit as st
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import time
import os
from datetime import datetime
import logging

class PropertyFinderStreamlitScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.all_listings = []
        self.max_consecutive_errors = 5
        self.consecutive_errors = 0

    def fetch_listings_from_page(self, url, page_number):
        """Fetch listings from a single page with enhanced error handling"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f"{url}&page={page_number}", 
                    headers=self.headers,
                    timeout=30
                )
                
                if response.status_code != 200:
                    raise requests.RequestException(f"Status code: {response.status_code}")

                soup = BeautifulSoup(response.content, "html.parser")
                next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})

                if not next_data_script:
                    raise ValueError("No __NEXT_DATA__ script found")

                json_content = next_data_script.string
                data = json.loads(json_content)
                listings = data["props"]["pageProps"]["searchResult"]["listings"]

                if listings:
                    self.consecutive_errors = 0
                    return listings
                else:
                    raise ValueError("No listings found in the response")

            except Exception as e:
                if attempt < max_retries - 1:
                    st.warning(f"Error on page {page_number}, attempt {attempt + 1}/{max_retries}: {str(e)}")
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    self.consecutive_errors += 1
                    st.error(f"Failed to fetch page {page_number} after {max_retries} attempts: {str(e)}")
                    return None

    def scrape(self, base_url, max_pages=100):
        """Main scraping function with Streamlit progress tracking"""
        self.all_listings = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        for page_number in range(1, max_pages + 1):
            status_text.text(f"Scraping page {page_number}...")
            
            # Check if we've had too many consecutive errors
            if self.consecutive_errors >= self.max_consecutive_errors:
                st.warning(f"Too many consecutive errors ({self.consecutive_errors}). Stopping scraping.")
                break
            
            listings = self.fetch_listings_from_page(base_url, page_number)

            if not listings:
                st.info("No more listings found or reached the end")
                break

            # Process the new listings
            self.all_listings.extend(listings)
            
            # Update progress
            progress_percentage = min(page_number / max_pages, 1.0)
            progress_bar.progress(progress_percentage)
            
            status_text.text(f"Total properties scraped: {len(self.all_listings)} on page {page_number}")
            
            time.sleep(1)  # Polite delay between requests

        progress_bar.empty()
        status_text.text(f"Scraping complete. Total properties: {len(self.all_listings)}")
        
        return self.all_listings

def main():
    st.title("PropertyFinder.ae Web Scraper")
    
    # Default URL if not provided
    default_url = 'https://www.propertyfinder.ae/en/search?c=1&fu=0&ob=mr'
    
    # URL input
    base_url = st.text_input(
        "Enter PropertyFinder Search URL", 
        value=default_url,
        help="Use the full search URL from PropertyFinder.ae"
    )
    
    # Max pages input
    max_pages = st.number_input(
        "Maximum number of pages to scrape", 
        min_value=1, 
        max_value=10000, 
        value=10
    )
    
    # Scrape button
    if st.button("Start Scraping"):
        scraper = PropertyFinderStreamlitScraper()
        
        try:
            listings = scraper.scrape(base_url, max_pages)
            
            if listings:
                # Convert to DataFrame
                df = pd.DataFrame(listings)
                
                # Display DataFrame
                st.subheader(f"Scraped {len(listings)} Properties")
                st.dataframe(df)
                
                # Download option
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='property_listings.csv',
                    mime='text/csv'
                )
            else:
                st.warning("No properties were scraped.")
        
        except Exception as e:
            st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

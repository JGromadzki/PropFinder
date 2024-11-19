import streamlit as st
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import time

class PropertyFinderStreamlitScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.all_listings = []
        self.max_consecutive_errors = 3

    def fetch_listings_from_page(self, url, page_number):
        """Fetch listings from a single page"""
        try:
            full_url = f"{url}&page={page_number}"
            response = requests.get(full_url, headers=self.headers, timeout=30)
            
            if response.status_code != 200:
                st.warning(f"Failed to fetch page {page_number}: Status code {response.status_code}")
                return None

            soup = BeautifulSoup(response.content, "html.parser")
            next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})

            if not next_data_script:
                st.warning(f"No data found on page {page_number}")
                return None

            json_content = next_data_script.string
            data = json.loads(json_content)
            listings = data["props"]["pageProps"]["searchResult"]["listings"]

            if not listings:
                return None

            return listings

        except Exception as e:
            st.warning(f"Error fetching page {page_number}: {str(e)}")
            return None

    def scrape(self, base_url):
        """Scrape all pages until no more listings are found"""
        self.all_listings = []
        page_number = 1
        consecutive_empty_pages = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()

        while consecutive_empty_pages < self.max_consecutive_errors:
            status_text.text(f"Scraping page {page_number}...")
            
            listings = self.fetch_listings_from_page(base_url, page_number)
            
            if listings is None:
                consecutive_empty_pages += 1
                st.warning(f"No listings found on page {page_number}. Attempts remaining: {self.max_consecutive_errors - consecutive_empty_pages}")
            else:
                self.all_listings.extend(listings)
                consecutive_empty_pages = 0
                
                # Update progress visualization
                status_text.text(f"Total properties scraped: {len(self.all_listings)} on page {page_number}")
            
            page_number += 1
            time.sleep(1)  # Polite delay between requests

        progress_bar.progress(1.0)
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
    
    # Scrape button
    if st.button("Start Scraping"):
        scraper = PropertyFinderStreamlitScraper()
        
        try:
            listings = scraper.scrape(base_url)
            
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

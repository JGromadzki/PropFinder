import streamlit as st
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import time
import numpy as np

class PropertyFinderScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        self.all_listings = []

    def process_listings_to_dataframe(self, listings):
        def flatten_dict(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    items.append((new_key, str(v) if v and isinstance(v[0], dict) else v))
                else:
                    items.append((new_key, v))
            return dict(items)

        processed_listings = []
        for listing in listings:
            try:
                flat_listing = flatten_dict(listing)
                processed_listings.append(flat_listing)
            except Exception as e:
                st.warning(f"Error processing listing: {str(e)}")

        df = pd.DataFrame(processed_listings)
        df = df.replace({np.nan: None})
        return df

    def fetch_listings_from_page(self, page_number):
        try:
            response = requests.get(
                self.base_url.format(page_number), 
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code != 200:
                st.error(f"Failed to fetch page {page_number}. Status code: {response.status_code}")
                return None

            soup = BeautifulSoup(response.content, "html.parser")
            next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})

            if not next_data_script:
                st.error("No data script found on the page")
                return None

            json_content = next_data_script.string
            data = json.loads(json_content)
            
            # Specific path to listings
            listings = data["props"]["pageProps"]["searchResult"].get("listings", [])

            # Check if there are actually properties on this page
            if not listings:
                return False

            return listings

        except Exception as e:
            st.error(f"Error fetching page {page_number}: {str(e)}")
            return None

    def scrape(self):
        self.all_listings = []
        page_number = 1

        while True:
            st.write(f"Scraping page {page_number}...")
            
            # Fetch listings for current page
            result = self.fetch_listings_from_page(page_number)
            
            # None means an error occurred
            if result is None:
                break
            
            # False means no more listings
            if result is False:
                st.info(f"No more listings found. Stopped at page {page_number-1}")
                break

            # Add listings and continue
            self.all_listings.extend(result)
            page_number += 1
            time.sleep(1)  # Polite delay between requests

        return self.process_listings_to_dataframe(self.all_listings)

def main():
    st.title("PropertyFinder Web Scraper")
    
    # Default URL with page number placeholder
    default_url = 'https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=mr&page={}'
    
    with st.form("scrape_form"):
        url = st.text_input("Scraping URL", value=default_url)
        submit_button = st.form_submit_button("Start Scraping")

    if submit_button:
        try:
            scraper = PropertyFinderScraper(url)
            df = scraper.scrape()
            
            # Display scraping statistics
            st.subheader("Scraping Results")
            st.write(f"Total Pages Scraped: {len(scraper.all_listings)}")
            st.write(f"Total Properties Found: {len(df)}")
            
            # Option to download data
            st.download_button(
                label="Download CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name='property_listings.csv',
                mime='text/csv'
            )
            
            # Preview of data
            st.subheader("Property Listings Preview")
            st.dataframe(df.head(10))
        
        except Exception as e:
            st.error(f"An error occurred during scraping: {str(e)}")

if __name__ == "__main__":
    main()

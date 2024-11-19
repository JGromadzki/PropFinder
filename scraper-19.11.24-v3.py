import streamlit as st
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import time
from datetime import datetime
import os
import numpy as np

class PropertyFinderScraper:
    def __init__(self):
        self.all_listings = []
        self.base_url = ''
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }

    def fetch_listings_from_page(self, url, page_number):
        try:
            response = requests.get(url.format(page_number), headers=self.headers, timeout=30)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.content, "html.parser")
            next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})

            if not next_data_script:
                return None

            json_content = next_data_script.string
            data = json.loads(json_content)
            listings = data["props"]["pageProps"]["searchResult"]["listings"]

            return listings if listings else None
        except Exception:
            return None

    def scrape(self, url, max_pages=1000):
        for page_number in range(1, max_pages + 1):
            listings = self.fetch_listings_from_page(url, page_number)
            if not listings:
                break
            self.all_listings.extend(listings)
            yield page_number, len(self.all_listings)

    def process_listings_to_dataframe(self):
        def flatten_dict(d, parent_key='', sep='_'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    items.append((new_key, str(v)))
                else:
                    items.append((new_key, v))
            return dict(items)

        processed_listings = []
        for listing in self.all_listings:
            processed_listings.append(flatten_dict(listing))
        df = pd.DataFrame(processed_listings).replace({np.nan: None})
        return df

# Streamlit App
st.title("PropertyFinder Scraper")

# Input field for the base URL
url = st.text_input("Enter the PropertyFinder URL for scraping (e.g., 'https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=mr&page={}')")

# Scraping logic
if url:
    scraper = PropertyFinderScraper()
    scraper.base_url = url
    st.write("Scraping in progress... Please wait.")
    progress_bar = st.progress(0)
    status_text = st.empty()
    scraped_data = pd.DataFrame()

    with st.spinner("Scraping..."):
        for page, total_properties in scraper.scrape(url):
            progress_bar.progress(min(page / 1000, 1.0))  # Update progress bar
            status_text.text(f"Pages scraped: {page} | Total properties scraped: {total_properties}")
            time.sleep(1)

        scraped_data = scraper.process_listings_to_dataframe()

    st.success("Scraping completed!")

    # Display data and download option
    if not scraped_data.empty:
        st.dataframe(scraped_data)
        csv = scraped_data.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="scraped_properties.csv",
            mime="text/csv",
        )
else:
    st.info("Please enter a valid PropertyFinder URL to start scraping.")

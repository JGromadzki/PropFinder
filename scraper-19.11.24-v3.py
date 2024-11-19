import streamlit as st
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

class PropertyFinderScraper:
    def __init__(self):
        self.base_url = ""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.all_listings = []

    def fetch_listings_from_page(self, page_number):
        """Fetch listings from a single page."""
        try:
            response = requests.get(self.base_url.format(page_number), headers=self.headers, timeout=30)
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

    def scrape(self, max_pages=1000):
        """Scrape all pages and yield progress."""
        for page_number in range(1, max_pages + 1):
            listings = self.fetch_listings_from_page(page_number)
            if not listings:
                break
            self.all_listings.extend(listings)
            yield page_number, len(self.all_listings)

    def process_listings_to_dataframe(self):
        """Flatten and convert listings to a DataFrame."""
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

        processed_listings = [flatten_dict(listing) for listing in self.all_listings]
        df = pd.DataFrame(processed_listings).replace({np.nan: None})
        return df

# Streamlit App
st.title("PropertyFinder Scraper")

# Input field for the base URL
url = st.text_input(
    "Enter the PropertyFinder URL (e.g., 'https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=mr&page={}'):"
)

# Scraping logic
if url:
    scraper = PropertyFinderScraper()
    scraper.base_url = url
    st.write("Scraping in progress... Please wait.")
    
    # Placeholders for dynamic updates
    pages_scraped_placeholder = st.empty()
    properties_scraped_placeholder = st.empty()

    pages_scraped = 0
    properties_scraped = 0
    scraped_data = None

    with st.spinner("Scraping..."):
        for page, total_properties in scraper.scrape():
            pages_scraped = page
            properties_scraped = total_properties

            # Update the placeholders dynamically
            pages_scraped_placeholder.write(f"**Pages Scraped:** {pages_scraped}")
            properties_scraped_placeholder.write(f"**Total Properties Scraped:** {properties_scraped}")

        # Process the scraped data into a DataFrame
        scraped_data = scraper.process_listings_to_dataframe()

    st.success("Scraping completed!")

    # Display number of pages and properties scraped
    st.write(f"**Pages Scraped:** {pages_scraped}")
    st.write(f"**Total Properties Scraped:** {properties_scraped}")

    # Display data and download option if data exists
    if scraped_data is not None and not scraped_data.empty:
        st.dataframe(scraped_data)
        csv = scraped_data.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="scraped_properties.csv",
            mime="text/csv",
        )
    else:
        st.warning("No data was scraped. Please check the URL or try again.")
else:
    st.info("Please enter a valid PropertyFinder URL to start scraping.")

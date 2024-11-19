import streamlit as st
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import tempfile
import os

# Configure the page
st.set_page_config(
    page_title="Property Scraper Dubai",
    page_icon="🏢",
    layout="wide"
)

# Custom CSS styling
st.markdown("""
    <style>
    .main { padding: 0rem 0rem; }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 5rem;
        padding-right: 5rem;
    }
    .stButton > button {
        width: 100%;
        border-radius: 5px;
        background-color: #000000;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)

def adjust_url_for_pagination(base_url, page_number):
    url_parts = list(urlparse(base_url))
    query_params = parse_qs(url_parts[4])
    query_params['page'] = [str(page_number)]
    url_parts[4] = urlencode(query_params, doseq=True)
    return urlunparse(url_parts)

def fetch_listings_from_page(base_url, page_number, headers):
    page_url = adjust_url_for_pagination(base_url, page_number)
    response = requests.get(page_url, headers=headers)
    if response.status_code != 200:
        st.write(f"Failed to retrieve page {page_number}")
        return None
    soup = BeautifulSoup(response.content, "html.parser")
    next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})
    if next_data_script:
        json_content = next_data_script.string
        data = json.loads(json_content)
        page_props = data["props"].get("pageProps", {})
        search_result = page_props.get("searchResult", {})
        listings = search_result.get("listings", [])
        return listings
    else:
        st.write(f"No listings found on page {page_number}")
        return None

def main():
    st.markdown("<h2 style='text-align:center;'>Dubai Property Listings Scraper</h2>", unsafe_allow_html=True)
    st.markdown("<p style='font-size: 1.2rem; text-align: center;'>Easily scrape property listings data from PropertyFinder.ae for analysis and insights.</p>", unsafe_allow_html=True)

    use_custom_link = st.checkbox("Use a custom URL")
    if use_custom_link:
        base_url = st.text_input("Paste custom URL", "https://www.propertyfinder.ae/en/search?l=1&c=2&t=1&fu=0&rp=y&ob=mr")
    else:
        base_url = 'https://www.propertyfinder.ae/en/search?l=1&c=2&t=1&fu=0&rp=y&ob=mr&page={}'

    scrape_button = st.button("Start Scraping", key="scrape-button")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }

    # Metrics placeholders
    properties_placeholder = st.empty()
    pages_placeholder = st.empty()

    if scrape_button:
        page_number = 1
        retry_count = 0
        all_listings = []
        total_properties = 0

        while True:
            listings = fetch_listings_from_page(base_url, page_number, headers)

            if not listings:
                retry_count += 1
                if retry_count >= 3:
                    st.write(f"No listings found after 3 retries on page {page_number}. Stopping.")
                    break
                else:
                    st.write(f"No listings found on page {page_number}. Retrying ({retry_count}/3)...")
                    time.sleep(2)
                    continue

            retry_count = 0
            all_listings.extend(listings)
            total_properties += len(listings)

            # Update metrics
            properties_placeholder.metric("Total Properties Scraped", total_properties)
            pages_placeholder.metric("Pages Scraped", page_number)

            page_number += 1
            time.sleep(1)

        if all_listings:
            df = pd.json_normalize(all_listings)

            # Filter out rows where property.id is null
            if 'property.id' in df.columns:
                df = df[df['property.id'].notnull()]
                st.write(f"Filtered listings: {len(df)} (after removing rows with null 'property.id')")

            # Allow column selection
            selected_columns = st.multiselect(
                "Choose columns to include in the CSV:",
                options=df.columns.tolist(),
                default=df.columns.tolist()
            )
            selected_df = df[selected_columns]

            # Write CSV to temporary file for automatic download
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
                selected_df.to_csv(tmp_file.name, index=False)
                tmp_file_path = tmp_file.name

            # Provide the file for download
            st.write(f"Scraping complete! The file is being downloaded automatically.")
            st.markdown(f"""
                <meta http-equiv="refresh" content="0; url=file://{tmp_file_path}">
            """, unsafe_allow_html=True)

            # Also display a button for manual download (optional)
            csv = selected_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name='property_listings_data.csv',
                mime='text/csv',
            )

if __name__ == "__main__":
    main()


import streamlit as st
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time

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
    .scraping-card-container {
        display: flex;
        justify-content: center;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .scraping-card {
        width: 25%;
        max-width: 300px;
        position: relative;
        background-image: url('https://assets-news.housing.com/news/wp-content/uploads/2018/06/24201341/HNIs-find-Dubai-property-more-attractive-than-Indian-real-estate-FB-1200x628-compressed.jpg');
        background-size: cover;
        background-position: center;
        height: 150px;
        border-radius: 10px;
        text-align: center;
        color: white;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        transition: transform 0.3s, box-shadow 0.3s;
    }
    .scraping-card:hover {
        transform: scale(1.05);
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    }
    .scraping-card h2 {
        font-size: 1.1rem;
        background-color: rgba(0, 0, 0, 0.6);
        padding: 0.5rem 1rem;
        border-radius: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

def adjust_url_for_pagination(base_url, page_number):
    # Parse the URL to extract its components
    url_parts = list(urlparse(base_url))
    query_params = parse_qs(url_parts[4])

    # Update or add the page parameter
    query_params['page'] = [str(page_number)]
    url_parts[4] = urlencode(query_params, doseq=True)

    # Reconstruct and return the updated URL
    return urlunparse(url_parts)

def main():
    # Header
    st.markdown("<h2 style='text-align:center;'>Dubai Property Listings Scraper</h2>", unsafe_allow_html=True)
    
    # Scraping Section
    st.markdown("<p style='font-size: 1.2rem; text-align: center;'>Easily scrape property listings data from PropertyFinder.ae for analysis and insights.</p>", unsafe_allow_html=True)
    
    # Choose custom link or predefined
    use_custom_link = st.checkbox("Use a custom URL")
    if use_custom_link:
        base_url = st.text_input("Paste custom URL", "https://www.propertyfinder.ae/en/search?l=1&c=2&t=1&fu=0&rp=y&ob=mr")
    else:
        base_url = 'https://www.propertyfinder.ae/en/search?l=1&c=2&t=1&fu=0&rp=y&ob=mr&page={}'

    # Centered Clickable Card to Start Scraping
    st.markdown("<div class='scraping-card-container'>", unsafe_allow_html=True)
    st.markdown(
        """
        <div class="scraping-card" onclick="document.getElementById('scrape-button').click();">
            <h2>Scrape Properties for Rent</h2>
        </div>
        """, unsafe_allow_html=True
    )
    st.markdown("</div>", unsafe_allow_html=True)

    # Start Scraping button
    scrape_button = st.button("Start Scraping", key="scrape-button")

    # Scraper Logic
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    all_listings = []

    def fetch_listings_from_page(page_number):
        # Adjust the base URL with the page number
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


# Execute scraping
if scrape_button:
    page_number = 1
    retry_count = 0  # Initialize retry count

    while True:
        listings = fetch_listings_from_page(page_number)

        if not listings:
            retry_count += 1
            if retry_count >= 3:  # Stop after 3 retries
                print(f"No listings found after 3 retries on page {page_number}. Stopping.")
                break
            else:
                print(f"No listings found on page {page_number}. Retrying ({retry_count}/3)...")
                time.sleep(2)  # Wait before retrying
                continue  # Retry the current page

        # Reset retry count if listings are found
        retry_count = 0
        all_listings.extend(listings)
        page_number += 1
        time.sleep(1)  # Optional delay to avoid overloading the server


        # Convert listings to DataFrame
        df = pd.json_normalize(all_listings)
        st.write(f"Total listings extracted: {len(df)}")
        st.dataframe(df.head(20))  # Display only the top 20 listings

        # Column selection before download
        selected_columns = st.multiselect("Choose columns to include in the CSV:", options=df.columns.tolist(), default=df.columns.tolist())
        selected_df = df[selected_columns]

        # Download CSV of selected columns
        csv = selected_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name='property_listings_data.csv',
            mime='text/csv',
        )

# Run main function
if __name__ == "__main__":
    main()

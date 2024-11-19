import streamlit as st
from scraper import PropertyFinderScraper  # Assume the scraper code is saved as `scraper.py`
import pandas as pd

# Streamlit App Configuration
st.title("Property Finder Scraper")
st.write("Input a URL to scrape property listings and view the results dynamically.")

# Input URL
url_input = st.text_input("Enter the Property Finder search URL", "")

# User Interaction
if st.button("Start Scraping"):
    if not url_input:
        st.error("Please enter a valid URL before starting the scraper.")
    else:
        # Initialize the scraper
        st.write("Starting the scraper...")
        scraper = PropertyFinderScraper(output_file='property_listings_data.csv', checkpoint_file='scraping_checkpoint.json')
        
        # Temporary list for progress tracking
        scraped_properties = []
        scraped_pages = 0

        # Callback function to fetch properties and update status
        def scrape_properties(scraper, max_pages=10000):
            nonlocal scraped_properties, scraped_pages
            try:
                scraper.base_url = url_input  # Override base URL with user input
                for page_number in range(scraper.last_page, max_pages + 1):
                    st.write(f"Scraping page {page_number}...")
                    listings = scraper.fetch_listings_from_page(page_number)
                    
                    if listings:
                        scraped_properties.extend(listings)
                        scraped_pages += 1
                        scraper.save_progress(page_number + 1)
                        st.write(f"Total properties scraped: {len(scraped_properties)}")
                    else:
                        st.write("No more listings found. Stopping scraper.")
                        break
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

        # Run the scraper
        scrape_properties(scraper)

        # Display results
        st.success("Scraping completed!")
        st.write(f"**Total Properties Scraped:** {len(scraped_properties)}")
        st.write(f"**Total Pages Scraped:** {scraped_pages}")

        # Option to view scraped data
        if len(scraped_properties) > 0:
            st.write("Scraped Data Preview:")
            df = pd.DataFrame(scraped_properties)
            st.dataframe(df)

# Footer
st.markdown("---")
st.markdown("Developed using Streamlit and BeautifulSoup.")

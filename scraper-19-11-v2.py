import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import time
import os
from datetime import datetime
import logging
import numpy as np

class PropertyFinderScraper:
    def __init__(self, output_file='property_listings_data.csv', checkpoint_file='scraping_checkpoint.json'):
        self.base_url = 'https://www.propertyfinder.ae/en/search?l=1&c=1&fu=0&ob=mr&page={}'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        self.checkpoint_file = checkpoint_file
        self.output_file = output_file
        self.all_listings = []
        self.last_page = 1
        self.max_consecutive_errors = 5
        self.consecutive_errors = 0
        self.setup_logging()
        self.load_checkpoint_and_data()

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraper.log')
            ]
        )

    def load_checkpoint_and_data(self):
        """Load both checkpoint and existing data"""
        # Load existing CSV data if it exists
        if os.path.exists(self.output_file):
            try:
                existing_df = pd.read_csv(self.output_file)
                self.all_listings = existing_df.to_dict('records')
                logging.info(f"Loaded {len(self.all_listings)} existing listings from {self.output_file}")
            except Exception as e:
                logging.error(f"Error loading existing data: {str(e)}")
                self.all_listings = []

        # Load checkpoint if it exists
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint_data = json.load(f)
                    self.last_page = checkpoint_data['last_page']
                logging.info(f"Resuming scraping from page {self.last_page}")
            except Exception as e:
                logging.error(f"Error loading checkpoint: {str(e)}")
                self.last_page = 1
        else:
            logging.info("Starting new scraping session")

    def save_progress(self, page_number):
        """Save both checkpoint and data"""
        # Save checkpoint
        checkpoint_data = {
            'last_page': page_number,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)

        # Save data to CSV
        if self.all_listings:
            # Flatten the JSON and expand columns
            df = self.process_listings_to_dataframe(self.all_listings)
            df.to_csv(self.output_file, index=False)
            logging.info(f"Progress saved: {len(df)} listings saved to {self.output_file}")

    def process_listings_to_dataframe(self, listings):
        """
        Process listings into a flattened DataFrame with expanded columns
        Handles nested dictionary structures and ensures consistent column extraction
        """
        def flatten_dict(d, parent_key='', sep='_'):
            """Recursively flatten nested dictionaries"""
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                
                # Handle nested dictionaries
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                # Handle lists 
                elif isinstance(v, list):
                    # Convert list to string for complex lists
                    items.append((new_key, str(v) if v and isinstance(v[0], dict) else v))
                else:
                    items.append((new_key, v))
            return dict(items)

        # Flatten and process each listing
        processed_listings = []
        for listing in listings:
            try:
                # Flatten the entire listing dictionary
                flat_listing = flatten_dict(listing)
                processed_listings.append(flat_listing)
            except Exception as e:
                logging.error(f"Error processing listing: {str(e)}")
                logging.error(f"Problematic listing: {listing}")

        # Create DataFrame
        df = pd.DataFrame(processed_listings)

        # Replace NaN with None for better CSV compatibility
        df = df.replace({np.nan: None})

        return df

    def fetch_listings_from_page(self, page_number):
        """Fetch listings from a single page with enhanced error handling"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    self.base_url.format(page_number), 
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
                    self.consecutive_errors = 0  # Reset error counter on success
                    return listings
                else:
                    raise ValueError("No listings found in the response")

            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Error on page {page_number}, attempt {attempt + 1}/{max_retries}: {str(e)}")
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    self.consecutive_errors += 1
                    logging.error(f"Failed to fetch page {page_number} after {max_retries} attempts: {str(e)}")
                    return None

    def scrape(self, max_pages=10000):
        """Main scraping function with automatic resume"""
        while True:  # Keep running until explicitly stopped
            try:
                for page_number in range(self.last_page, max_pages + 1):
                    logging.info(f"Fetching page {page_number}...")
                    
                    # Check if we've had too many consecutive errors
                    if self.consecutive_errors >= self.max_consecutive_errors:
                        logging.warning(f"Too many consecutive errors ({self.consecutive_errors}). Waiting 5 minutes before resuming...")
                        time.sleep(300)  # Wait 5 minutes
                        self.consecutive_errors = 0  # Reset error counter
                        continue  # Try the same page again
                    
                    listings = self.fetch_listings_from_page(page_number)

                    if not listings:
                        if self.consecutive_errors >= self.max_consecutive_errors:
                            continue  # Try again after waiting
                        else:
                            logging.info("No more listings found or reached the end")
                            break

                    # Process and save the new listings
                    self.all_listings.extend(listings)
                    self.save_progress(page_number + 1)
                    
                    logging.info(f"Total listings collected: {len(self.all_listings)}")
                    time.sleep(1)  # Polite delay between requests

                # If we've completed successfully, break the while loop
                logging.info("Scraping completed successfully!")
                break

            except KeyboardInterrupt:
                logging.info("\nScraping interrupted by user")
                self.save_progress(page_number)
                raise  # Re-raise to allow clean exit

            except Exception as e:
                logging.error(f"Unexpected error occurred: {str(e)}")
                self.save_progress(page_number)
                logging.info("Waiting 5 minutes before resuming...")
                time.sleep(300)  # Wait 5 minutes before resuming
                continue  # Resume from the last saved point

# Usage
if __name__ == "__main__":
    scraper = PropertyFinderScraper()
    try:
        scraper.scrape()
    except KeyboardInterrupt:
        logging.info("Scraping terminated by user. Progress has been saved.")
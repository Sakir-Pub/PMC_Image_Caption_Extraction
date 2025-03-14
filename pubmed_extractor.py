import os
import json
import requests
import xml.etree.ElementTree as ET
import urllib.request
from PIL import Image
from io import BytesIO
import time
import re
from concurrent.futures import ThreadPoolExecutor

class PMCImageTextExtractor:
    """
    Extract figure images and captions from PubMed Central articles.
    """
    
    def __init__(self, output_dir="pmc_dataset", email="your_email@example.com", api_key=None):
        """
        Initialize the extractor.
        
        Args:
            output_dir: Directory to save the dataset
            email: Your email for NCBI E-utilities (they may block excessive usage without identification)
            api_key: NCBI API key (optional, but recommended for higher rate limits)
        """
        self.output_dir = output_dir
        self.image_dir = os.path.join(output_dir, "images")
        self.metadata_file = os.path.join(output_dir, "metadata.json")
        self.email = email
        self.api_key = api_key
        
        # Create directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.image_dir, exist_ok=True)
        
        # Initialize metadata
        self.metadata = {"pairs": []}
        
    def search_articles(self, query, limit_results=True, max_results=100):
        """
        Search for PMC articles based on a query.
        
        Args:
            query: Search query (e.g., "cancer immunotherapy")
            limit_results: Whether to limit the number of results
            max_results: Maximum number of results to return if limit_results is True
            
        Returns:
            List of PMC IDs
        """
        # Build the search URL
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        # Add "open access"[filter] to limit to open access articles
        full_query = f"{query} AND open access[filter]"
        
        # First get the total count
        count_params = {
            "db": "pmc",
            "term": full_query,
            "rettype": "count",
            "tool": "ImageTextPairExtractor",
            "email": self.email
        }
        
        if self.api_key:
            count_params["api_key"] = self.api_key
        
        # Get the total count first
        count_response = requests.get(base_url, params=count_params)
        if count_response.status_code != 200:
            print(f"Error counting articles: {count_response.status_code}")
            return []
        
        total_articles = int(count_response.text.strip())
        print(f"Total articles found matching the query: {total_articles}")
        
        # If not limiting results, set max_results to total_articles
        if not limit_results:
            max_results = total_articles
            print(f"No limit applied - will attempt to process all {total_articles} articles")
        else:
            print(f"Limiting results to {max_results} articles")
        
        # Now get the actual results
        params = {
            "db": "pmc",
            "term": full_query,
            "retmax": max_results,
            "retmode": "json",
            "tool": "ImageTextPairExtractor",
            "email": self.email
        }
        
        if self.api_key:
            params["api_key"] = self.api_key
            
        # Make the request
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Error searching articles: {response.status_code}")
            return []
            
        # Extract PMC IDs
        data = response.json()
        return data["esearchresult"]["idlist"]
    
    def fetch_article(self, pmc_id):
        """
        Fetch full article XML from PMC.
        
        Args:
            pmc_id: PMC ID of the article
            
        Returns:
            XML string if successful, None otherwise
        """
        # Build the fetch URL
        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        params = {
            "db": "pmc",
            "id": pmc_id,
            "retmode": "xml",
            "tool": "ImageTextPairExtractor",
            "email": self.email
        }
        
        if self.api_key:
            params["api_key"] = self.api_key
            
        # Make the request
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching article {pmc_id}: {response.status_code}")
            return None
            
        return response.text
    
    def extract_figure_data(self, xml_string, pmc_id):
        """
        Extract figures and captions from the article XML.
        
        Args:
            xml_string: XML content of the article
            pmc_id: PMC ID of the article
            
        Returns:
            List of dictionaries containing figure data
        """
        try:
            # Parse XML
            root = ET.fromstring(xml_string)
            
            # Define namespaces (PMC XML uses namespaces)
            namespaces = {
                "xlink": "http://www.w3.org/1999/xlink",
                "mml": "http://www.w3.org/1998/Math/MathML"
            }
            
            # Find all figures
            figures = []
            
            # Look for different figure elements (PMC XML structure can vary)
            fig_elements = root.findall(".//fig") + root.findall(".//fig-group/fig")
            
            for fig in fig_elements:
                figure_data = {"pmc_id": pmc_id}
                
                # Extract figure ID
                fig_id = fig.get("id", f"fig_{len(figures)}")
                figure_data["figure_id"] = fig_id
                
                # Extract caption
                caption_element = fig.find(".//caption")
                if caption_element is not None:
                    # Get all text content in the caption
                    caption_text = "".join(caption_element.itertext()).strip()
                    figure_data["caption"] = caption_text
                    
                    # Check if caption contains relevant keywords: fundus, oct, or octa
                    if not (re.search(r'fundus|oct|octa', caption_text, re.IGNORECASE)):
                        # Skip figures without relevant keywords in caption
                        continue
                else:
                    # Skip figures without captions
                    continue
                
                # Extract image information
                graphic_element = fig.find(".//graphic")
                if graphic_element is not None:
                    # Get the image link
                    href = graphic_element.get("{http://www.w3.org/1999/xlink}href")
                    if href:
                        figure_data["image_href"] = href
                        figures.append(figure_data)
                
            return figures
            
        except Exception as e:
            print(f"Error extracting figures from {pmc_id}: {str(e)}")
            return []
    
    def download_figure_image(self, figure_data, pmc_id):
        """
        Download the figure image and save it.
        """
        try:
            # Get the image reference from the XML
            image_href = figure_data["image_href"]
            figure_id = figure_data["figure_id"]
            
            # Set up headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://www.ncbi.nlm.nih.gov/',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            
            # Access the HTML page of the article
            article_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/"
            response = requests.get(article_url, headers=headers)
            
            if response.status_code != 200:
                print(f"Error accessing article page {article_url}: {response.status_code}")
                return None
            
            # Use regex to find the image URL in the HTML
            html_content = response.text
            # Look for the image with the corresponding href/id
            pattern = f'<img[^>]*?src="([^"]*?{re.escape(image_href)}[^"]*?)"'
            matches = re.findall(pattern, html_content)
            
            if not matches:
                # Try an alternative pattern that might match the figure in the HTML
                pattern = f'<img[^>]*?data-figure-id="{re.escape(figure_id)}"[^>]*?src="([^"]*?)"'
                matches = re.findall(pattern, html_content)
                
                if not matches:
                    print(f"Could not find image URL for {image_href} in article PMC{pmc_id}")
                    return None
            
            image_url = matches[0]
            # If the URL is relative, make it absolute
            if image_url.startswith('/'):
                image_url = f"https://www.ncbi.nlm.nih.gov{image_url}"
            
            # Download the image with the same headers
            response = requests.get(image_url, headers=headers)
            
            if response.status_code != 200:
                print(f"Error downloading image {image_url}: {response.status_code}")
                return None
                
            # Save the image
            img = Image.open(BytesIO(response.content))
            image_filename = f"PMC{pmc_id}_{figure_data['figure_id']}.jpg"
            image_path = os.path.join(self.image_dir, image_filename)
            img.save(image_path)
            
            # Update figure data
            figure_data["local_image_path"] = image_path
            return figure_data
            
        except Exception as e:
            print(f"Error processing image for {pmc_id}: {str(e)}")
            return None
    
    def process_article(self, pmc_id):
        """
        Process a single article to extract all figure-caption pairs.
        
        Args:
            pmc_id: PMC ID of the article
            
        Returns:
            Number of figure-caption pairs extracted
        """
        # Fetch article XML
        xml_string = self.fetch_article(pmc_id)
        if not xml_string:
            return 0
            
        # Extract figures and captions
        figures = self.extract_figure_data(xml_string, pmc_id)
        
        # Download and save images
        pairs_count = 0
        for figure in figures:
            # Respect rate limits
            time.sleep(0.5)
            
            # Download image
            processed_figure = self.download_figure_image(figure, pmc_id)
            if processed_figure:
                # Add to metadata
                pair_data = {
                    "image_path": os.path.basename(processed_figure["local_image_path"]),
                    "caption": processed_figure["caption"],
                    "pmc_id": pmc_id,
                    "figure_id": processed_figure["figure_id"]
                }
                self.metadata["pairs"].append(pair_data)
                pairs_count += 1
                
        return pairs_count
    
    def create_dataset(self, query, limit_articles=True, max_articles=10):
        """
        Create a dataset of figure-caption pairs from PMC articles.
        
        Args:
            query: Search query for relevant articles
            limit_articles: Whether to limit the number of articles to process
            max_articles: Maximum number of articles to process if limit_articles is True
            
        Returns:
            Path to the dataset directory
        """
        print(f"Searching for articles with query: '{query}'")
        pmc_ids = self.search_articles(query, limit_results=limit_articles, max_results=max_articles)
        
        if not pmc_ids:
            print("No articles found.")
            return self.output_dir
            
        print(f"Will process {len(pmc_ids)} articles. Starting processing...")
        
        total_pairs = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i, pmc_id in enumerate(pmc_ids):
                print(f"Processing article {i+1}/{len(pmc_ids)}: PMC{pmc_id}")
                pairs = self.process_article(pmc_id)
                total_pairs += pairs
                print(f"  Extracted {pairs} figure-caption pairs")
                
                # Save metadata periodically
                if i % 5 == 0 or i == len(pmc_ids) - 1:
                    with open(self.metadata_file, 'w') as f:
                        json.dump(self.metadata, f, indent=2)
                        
                # Respect rate limits
                time.sleep(1)
        
        print(f"Dataset creation complete. Total pairs: {total_pairs}")
        print(f"Dataset saved to: {self.output_dir}")
        print(f"Metadata file: {self.metadata_file}")
        
        return self.output_dir

# Example usage
if __name__ == "__main__":
    # Initialize the extractor
    extractor = PMCImageTextExtractor(
        output_dir="pmc_glaucoma",
        email="anabil@charlotte.edu",  # Replace with your email
        api_key="c723f4433947bd8c33ac66bcb5e5c58c4608"
    )
    
    # Create the dataset
    # Set limit_articles=False to process all available articles
    extractor.create_dataset(
        query="glaucoma",  # Your topic of interest
        limit_articles=False,  # Set to False to process all available articles
        max_articles=100  # Only used if limit_articles is True
    )
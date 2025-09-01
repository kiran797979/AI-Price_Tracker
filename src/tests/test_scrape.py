from firecrawl import FirecrawlApp
from dotenv import load_dotenv

load_dotenv()

app = FirecrawlApp()

ebay_item = "https://barnerbrand.com/products/holly-glossy"

# Test scrape the ebay item
data = app.scrape_url(ebay_item)
# print(data["screenshot"])

from typing import Tuple
from datetime import datetime
from urllib.parse import urlparse

from firecrawl import FirecrawlApp
from src.domain.models import ProductCreate, PriceHistoryCreate
from src.infrastructure.repositories.product_repository import ProductRepository


class ProductService:
    def __init__(self, product_repository: ProductRepository):
        self.repository = product_repository
        self.firecrawl = FirecrawlApp()

    async def add_product(self, url: str) -> Tuple[bool, str]:
        """Add a new product to track"""
        if not self._validate_url(url):
            return False, "Please enter a valid URL"

        try:
            # Check if product exists
            existing_product = self.repository.get(url)
            if existing_product:
                return False, "Product already being tracked!"

            # Scrape product
            scraped_product = await self._scrape_product(url)

            # Create product
            product = self.repository.add(scraped_product)

            # Add initial price history
            price_history = PriceHistoryCreate(
                product_url=product.url, price=product.price, product_name=product.name
            )
            self.repository.add_price_history(price_history)

            return (
                True,
                f"Added and checked initial price for: {product.name} - ${product.price:.2f}",
            )

        except Exception as e:
            print(f"Error: {str(e)}")
            return False, f"Error adding product: {str(e)}"

    def _validate_url(self, url: str) -> bool:
        """Validate URL format"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    async def _scrape_product(self, url: str) -> ProductCreate:
        """Scrape product details"""
        data = self.firecrawl.scrape_url(
            url,
            params={
                "formats": ["extract"],
                "extract": {"schema": ProductCreate.model_json_schema()},
            },
        )
        product_data = data["extract"]
        product_data["url"] = url  # Use original URL
        product_data["check_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        return ProductCreate(**product_data)

    def remove_product(self, url: str) -> None:
        """Remove a product and its price history"""
        product = self.repository.get(url)
        if product:
            self.repository.delete(url)

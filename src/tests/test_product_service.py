import pytest
from unittest.mock import Mock, patch
from sqlalchemy import text
from src.infrastructure.repositories.product_repository import ProductRepository
from src.infrastructure.database import SessionLocal
from src.services.product_service import ProductService
from src.domain.models import ProductCreate


@pytest.fixture(autouse=True)
def cleanup_database():
    session = SessionLocal()
    try:
        # Delete all existing products and price history
        session.execute(text("DELETE FROM price_history"))
        session.execute(text("DELETE FROM products"))
        session.commit()
    finally:
        session.close()


@pytest.fixture
def session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def repository(session):
    return ProductRepository(session)


@pytest.fixture
def mock_firecrawl():
    with patch("services.product_service.FirecrawlApp") as mock:
        app_instance = Mock()
        app_instance.scrape_url.return_value = {
            "extract": {
                "url": "https://www.amazon.com/dp/B09HMV6K1W",
                "name": "Test Product",
                "price": 99.99,
                "currency": "USD",
                "main_image_url": "https://example.com/image.jpg",
            }
        }
        mock.return_value = app_instance
        yield mock


@pytest.fixture
def service(repository, mock_firecrawl):
    return ProductService(repository)


@pytest.mark.asyncio
async def test_add_product(service):
    # Test invalid URL
    success, message = await service.add_product("not-a-url")
    assert not success
    assert message == "Please enter a valid URL"

    # Test valid URL
    test_url = "https://www.example.com/product/123"
    success, message = await service.add_product(test_url)
    assert success, f"Failed to add product: {message}"
    assert "Added and checked initial price for:" in message

    # Test duplicate product
    success, message = await service.add_product(test_url)
    assert not success
    assert message == "Product already being tracked!"

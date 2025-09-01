import pytest
from unittest.mock import Mock, patch
from sqlalchemy import text
from datetime import datetime

from src.infrastructure.repositories.product_repository import ProductRepository
from src.infrastructure.database import SessionLocal
from src.services.price_service import PriceService
from src.domain.models import Product, PriceHistoryCreate


@pytest.fixture(autouse=True)
def cleanup_database():
    session = SessionLocal()
    try:
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
    with patch("services.price_service.FirecrawlApp") as mock:
        app_instance = Mock()

        # Create a mock coroutine for price drop test
        async def mock_scrape_drop(*args, **kwargs):
            return {
                "extract": {
                    "url": "https://www.amazon.com/dp/B09HMV6K1W",
                    "name": "Test Product",
                    "price": 79.99,  # Lower price to trigger alert
                    "currency": "USD",
                    "main_image_url": "https://example.com/image.jpg",
                }
            }

        # Create a mock coroutine for no price drop test
        async def mock_scrape_no_drop(*args, **kwargs):
            return {
                "extract": {
                    "url": "https://www.amazon.com/dp/B09HMV6K1W",
                    "name": "Test Product",
                    "price": 99.99,  # Same price as initial
                    "currency": "USD",
                    "main_image_url": "https://example.com/image.jpg",
                }
            }

        app_instance.scrape_url = mock_scrape_drop  # Default to price drop scenario
        mock.return_value = app_instance

        def switch_to_no_drop():
            app_instance.scrape_url = mock_scrape_no_drop

        mock.switch_to_no_drop = switch_to_no_drop
        yield mock


@pytest.fixture
def service(repository, mock_firecrawl):
    return PriceService(repository)


@pytest.mark.asyncio
async def test_check_prices_no_products(service):
    """Test checking prices when no products exist"""
    updated_products = await service.check_prices()
    assert len(updated_products) == 0


@pytest.mark.asyncio
async def test_check_prices_with_price_drop(service, repository):
    """Test checking prices with a price drop that triggers alert"""
    # Add a test product
    test_product = Product(
        url="https://www.amazon.com/dp/B09HMV6K1W",
        name="Test Product",
        price=99.99,
        currency="USD",
        check_date=datetime.now().isoformat(),
        main_image_url="https://example.com/image.jpg",
    )
    repository.add(test_product)

    # Add initial price history
    price_history = PriceHistoryCreate(
        product_url=test_product.url,
        price=test_product.price,
        product_name=test_product.name,
    )
    repository.add_price_history(price_history)

    # Mock the send_price_alert function
    with patch("services.price_service.send_price_alert") as mock_alert:
        mock_alert.return_value = None  # Mock the coroutine return
        updated_products = await service.check_prices()

        # Verify results
        assert len(updated_products) == 1
        assert mock_alert.called
        mock_alert.assert_called_once_with(
            "Test Product", 99.99, 79.99, "https://www.amazon.com/dp/B09HMV6K1W"
        )

        # Verify new price history was added
        histories = repository.get_price_history(test_product.url)
        assert len(histories) == 2
        assert histories[-1].price == 79.99


@pytest.mark.asyncio
async def test_check_prices_no_price_drop(service, repository, mock_firecrawl):
    """Test checking prices without a price drop"""
    # Switch to no price drop scenario
    mock_firecrawl.switch_to_no_drop()

    # Add a test product
    test_product = Product(
        url="https://www.amazon.com/dp/B09HMV6K1W",
        name="Test Product",
        price=99.99,
        currency="USD",
        check_date=datetime.now().isoformat(),
        main_image_url="https://example.com/image.jpg",
    )
    repository.add(test_product)

    # Add initial price history
    price_history = PriceHistoryCreate(
        product_url=test_product.url,
        price=test_product.price,
        product_name=test_product.name,
    )
    repository.add_price_history(price_history)

    # Mock the send_price_alert function
    with patch("services.price_service.send_price_alert") as mock_alert:
        updated_products = await service.check_prices()

        # Verify results
        assert len(updated_products) == 1
        assert not mock_alert.called

        # Verify new price history was added
        histories = repository.get_price_history(test_product.url)
        assert len(histories) == 2
        assert histories[-1].price == 99.99

from src.infrastructure.repositories.product_repository import ProductRepository
from src.infrastructure.database import SessionLocal
from src.domain.models import ProductCreate, PriceHistoryCreate
from datetime import datetime

# Create a session
session = SessionLocal()

# Create repository
repo = ProductRepository(session)

try:
    # Test getting all products
    products = repo.get_all()
    print(f"Found {len(products)} products")

    # Test adding a product
    if len(products) == 0:
        test_product = ProductCreate(
            url="https://example.com/test",
            name="Test Product",
            price=99.99,
            currency="USD",
            main_image_url="https://example.com/image.jpg",
        )
        product = repo.add(test_product)
        print(f"Added test product: {product.name}")

        # Test adding price history
        price_history = PriceHistoryCreate(
            product_url=product.url, price=product.price, product_name=product.name
        )
        history = repo.add_price_history(price_history)
        print(f"Added price history: {history.price}")

    print("All tests passed!")
finally:
    session.close()

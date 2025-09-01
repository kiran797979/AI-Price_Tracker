import asyncio
import sys
import os

# Add the src directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.services.notifications import send_price_alert


async def test():
    await send_price_alert(
        product_name="Test Product",
        old_price=99.99,
        new_price=79.99,
        url="https://www.amazon.com/dp/B09HMV6K1W",
    )


if __name__ == "__main__":
    asyncio.run(test())

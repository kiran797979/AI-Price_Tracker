import asyncio
from sqlalchemy import text
from src.infrastructure.database import get_session


async def cleanup_database():
    """Clean up all data from the database"""
    session = next(get_session())
    try:
        # Delete price history first to avoid foreign key constraint violations
        session.execute(text("DELETE FROM price_histories"))
        session.commit()

        # Then delete products
        session.execute(text("DELETE FROM products"))
        session.commit()
        print("Database cleaned successfully!")
    except Exception as e:
        print(f"Error cleaning database: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    asyncio.run(cleanup_database())

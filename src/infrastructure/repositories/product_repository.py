from datetime import datetime
from typing import List, Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

from src.domain.models import Product, ProductCreate, PriceHistory, PriceHistoryCreate
from .base import BaseRepository
from ..database.models import Product as DBProduct, PriceHistory as DBPriceHistory


class ProductRepository(BaseRepository[Product]):
    def __init__(self, session: Session):
        self.session = session

    def _to_domain(self, db_product: DBProduct) -> Product:
        """Convert DB model to domain model"""
        return Product.model_validate(db_product)

    def _to_db(self, product: ProductCreate) -> DBProduct:
        """Convert domain model to DB model"""
        return DBProduct(**product.model_dump())

    def add(self, product: ProductCreate) -> Product:
        """Add a new product"""
        db_product = self._to_db(product)
        self.session.add(db_product)
        self.session.commit()
        return self._to_domain(db_product)

    def get(self, id: str) -> Optional[Product]:
        """Get a product by URL (our ID)"""
        db_product = self.session.query(DBProduct).filter_by(url=id).first()
        return self._to_domain(db_product) if db_product else None

    def get_all(self) -> List[Product]:
        """Get all products"""
        db_products = self.session.query(DBProduct).all()
        return [self._to_domain(p) for p in db_products]

    def delete(self, id: str) -> None:
        """Delete a product and its price history"""
        product = self.session.query(DBProduct).filter_by(url=id).first()
        if product:
            self.session.query(DBPriceHistory).filter_by(product_url=id).delete()
            self.session.delete(product)
            self.session.commit()

    def _to_price_history_domain(
        self, db_price_history: DBPriceHistory
    ) -> PriceHistory:
        """Convert DB price history to domain model"""
        return PriceHistory.model_validate(db_price_history)

    def _to_price_history_db(self, price_history: PriceHistoryCreate) -> DBPriceHistory:
        """Convert domain price history to DB model"""
        return DBPriceHistory(**price_history.model_dump())

    def get_price_history(self, product_url: str) -> List[PriceHistory]:
        """Get price history for a product"""
        db_histories = (
            self.session.query(DBPriceHistory)
            .filter_by(product_url=product_url)
            .order_by(DBPriceHistory.timestamp.asc())
            .all()
        )
        return [self._to_price_history_domain(h) for h in db_histories]

    def add_price_history(self, price_history: PriceHistoryCreate) -> PriceHistory:
        """Add a new price history entry"""
        db_price_history = self._to_price_history_db(price_history)
        self.session.add(db_price_history)
        self.session.commit()
        return self._to_price_history_domain(db_price_history)

    def update(self, product: Product) -> Product:
        """Update a product in the database"""
        db_product = (
            self.session.query(DBProduct).filter(DBProduct.url == product.url).first()
        )
        if db_product:
            db_product.price = product.price
            db_product.name = product.name
            db_product.currency = product.currency
            db_product.main_image_url = product.main_image_url
            db_product.check_date = datetime.now().isoformat()
            self.session.commit()
            return product
        raise ValueError(f"Product with URL {product.url} not found")

import os
import sys

import streamlit as st

from src.infrastructure.database import get_session
from src.infrastructure.repositories.product_repository import ProductRepository
from src.presentation.components.product_list import ProductList
from src.presentation.components.sidebar import Sidebar
from src.services.price_service import PriceService
from src.services.product_service import ProductService


def init_services():
    """Initialize services with dependencies"""
    session = next(get_session())
    repository = ProductRepository(session)
    product_service = ProductService(repository)
    price_service = PriceService(repository)
    return product_service, price_service


def render_dashboard(product_service: ProductService, price_service: PriceService):
    st.title("Price Tracker Dashboard")

    # Give a brief info about the app
    st.markdown(
        """##### Track product prices across e-commerce sites and get Discord notifications when prices drop. See setup instructions in the [GitHub repo](https://github.com/kiran797979/AI-Price_Tracker). View my tracked products below.
        """
    )

    # Render sidebar
    sidebar = Sidebar(product_service)
    sidebar.render()

    # Main content
    st.header("Tracked Products")
    st.markdown("---")

    products = product_service.repository.get_all()

    if not products:
        st.info("No products are being tracked. Add some using the sidebar!")
    else:
        product_list = ProductList(product_service)
        product_list.render(products)


def main():
    st.set_page_config(page_title="Price Tracker", page_icon="📊", layout="wide")

    # Disable file watcher in Streamlit Cloud
    if os.getenv("STREAMLIT_SERVER_ADDRESS"):
        st.set_option("server.fileWatcherType", "none")

    # Initialize services
    product_service, price_service = init_services()

    # Render dashboard
    render_dashboard(product_service, price_service)


if __name__ == "__main__":
    main()

import streamlit as st
import asyncio
from src.services.product_service import ProductService


class Sidebar:
    def __init__(self, product_service: ProductService):
        self.product_service = product_service

    def render(self):
        st.sidebar.header("Add New Product")
        new_url = st.sidebar.text_input("Product URL")

        if st.sidebar.button("Add Product") and new_url:
            success, message = asyncio.run(self.product_service.add_product(new_url))
            if success:
                st.sidebar.success(message)
                st.rerun()
            else:
                st.sidebar.error(message)

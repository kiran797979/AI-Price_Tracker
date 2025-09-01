import os
import streamlit as st
from dotenv import load_dotenv
from src.presentation.app import main
from src.infrastructure.database.session import get_db_url

if __name__ == "__main__":
    # Force reload environment variables
    load_dotenv(override=True)

    # Debug database connection
    db_url = get_db_url()
    if not db_url or "postgres" not in db_url:
        st.error("Invalid database URL. Please check your .env file.")
        st.stop()

    main()

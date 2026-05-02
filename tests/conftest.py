"""Root conftest — loads .env so provider credentials are available for integration tests."""

from dotenv import load_dotenv

load_dotenv()

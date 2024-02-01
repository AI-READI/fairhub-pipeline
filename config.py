"""Configuration for the application."""

from os import environ
from pathlib import Path

from dotenv import dotenv_values

# Check if `.env` file exists
env_path = Path(".") / ".env"

LOCAL_ENV_FILE = env_path.exists()

# Load environment variables from .env
config = dotenv_values(".env")


def get_env(key, optional=False):
    """Return environment variable from .env or native environment."""
    if LOCAL_ENV_FILE:
        return config.get(key)

    if key not in environ and not optional:
        raise ValueError(f"Environment variable {key} not set.")

    return environ.get(key)


FAIRHUB_ACCESS_TOKEN = get_env("FAIRHUB_ACCESS_TOKEN")

FAIRHUB_DATABASE_NAME = get_env("FAIRHUB_DATABASE_NAME", optional=True)
FAIRHUB_DATABASE_PASSWORD = get_env("FAIRHUB_DATABASE_PASSWORD", optional=True)
FAIRHUB_DATABASE_HOST = get_env("FAIRHUB_DATABASE_HOST", optional=True)
FAIRHUB_DATABASE_USER = get_env("FAIRHUB_DATABASE_USER", optional=True)
FAIRHUB_DATABASE_PORT = get_env("FAIRHUB_DATABASE_PORT", optional=True)

FAIRHUB_ENVIRONMENT = get_env("FAIRHUB_ENVIRONMENT")

DATACITE_CREDENTIALS = get_env("DATACITE_CREDENTIALS", optional=True)

AZURE_STORAGE_ACCESS_KEY = get_env("AZURE_STORAGE_ACCESS_KEY")
AZURE_STORAGE_CONNECTION_STRING = get_env("AZURE_STORAGE_CONNECTION_STRING")

DATACITE_API_URL = "https://api.datacite.org"

if FAIRHUB_ENVIRONMENT in ["staging", "development"]:
    # Using the test environment for DataCite
    DATACITE_API_URL = "https://api.test.datacite.org"

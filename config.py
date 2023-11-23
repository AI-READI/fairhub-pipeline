"""Configuration for the application."""
from os import environ
from pathlib import Path

from dotenv import dotenv_values

# Check if `.env` file exists
env_path = Path(".") / ".env"

LOCAL_ENV_FILE = env_path.exists()

# Load environment variables from .env
config = dotenv_values(".env")


def get_env(key):
    """Return environment variable from .env or native environment."""
    if LOCAL_ENV_FILE:
        return config.get(key)

    if key not in environ:
        raise ValueError(f"Environment variable {key} not set.")

    return environ.get(key)


FAIRHUB_ACCESS_TOKEN = get_env("FAIRHUB_ACCESS_TOKEN")
AZURE_STORAGE_ACCESS_KEY = get_env("AZURE_STORAGE_ACCESS_KEY")
AZURE_STORAGE_CONNECTION_STRING = get_env("AZURE_STORAGE_CONNECTION_STRING")

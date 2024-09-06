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
FAIRHUB_PIPELINE_URL = "https://pipeline.fairhub.io/api"

if FAIRHUB_ENVIRONMENT in ["staging", "dev"]:
    # Using the test environment for DataCite
    DATACITE_API_URL = "https://api.test.datacite.org"

if FAIRHUB_ENVIRONMENT == "staging":
    FAIRHUB_PIPELINE_URL = "https://staging.pipeline.fairhub.io/api"
elif FAIRHUB_ENVIRONMENT == "dev":
    FAIRHUB_PIPELINE_URL = "http://localhost:7071/api"

FAIRHUB_CATCH_ALL_LOG_ENDPOINT = get_env("FAIRHUB_CATCH_ALL_LOG_ENDPOINT")
FAIRHUB_TRITON_LOG_ENDPOINT = get_env("FAIRHUB_TRITON_LOG_ENDPOINT", optional=True)
FAIRHUB_MAESTRO2_LOG_ENDPOINT = get_env("FAIRHUB_MAESTRO2_LOG_ENDPOINT", optional=True)
FAIRHUB_EIDON_LOG_ENDPOINT = get_env("FAIRHUB_EIDON_LOG_ENDPOINT", optional=True)
FAIRHUB_ECG_LOG_ENDPOINT = get_env("FAIRHUB_ECG_LOG_ENDPOINT", optional=True)
FAIRHUB_CGM_LOG_ENDPOINT = get_env("FAIRHUB_CGM_LOG_ENDPOINT", optional=True)
FAIRHUB_FITNESS_TRACKER_LOG_ENDPOINT = get_env(
    "FAIRHUB_FITNESS_TRACKER_LOG_ENDPOINT", optional=True
)
FAIRHUB_SPECTRALIS_LOG_ENDPOINT = get_env(
    "FAIRHUB_SPECTRALIS_LOG_ENDPOINT", optional=True
)
FAIRHUB_FLIO_LOG_ENDPOINT = get_env("FAIRHUB_FLIO_LOG_ENDPOINT", optional=True)

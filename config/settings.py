import os

# API Configuration - Using environment variable for API key
API_KEY_ENV_VAR = "GEMINI_API_KEY"

# Processing limits - More descriptive variable names
DOWNLOAD_MAX_RETRIES = 3
MAX_TOKENS_PER_PROMPT = 4000
TRANSLATION_INTERVAL_SECONDS = 66


# Logging Configuration
LOG_LEVEL = "INFO"

def get_api_key() -> str:
    """Retrieve API key from environment variable, raising error if not set."""
    api_key = os.environ.get(API_KEY_ENV_VAR)
    if not api_key:
        raise EnvironmentError(
            f"API key not found. Set environment variable: {API_KEY_ENV_VAR}"
        )
    return api_key

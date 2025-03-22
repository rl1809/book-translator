from dataclasses import dataclass
from typing import Dict, Any
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from PyQt5.QtCore import QSettings


def get_generation_config() -> Dict[str, Any]:
    """Get generation config from settings or return defaults."""
    settings = QSettings("NovelTranslator", "Config")
    
    return {
        "temperature": settings.value("ModelTemperature", 0.0, type=float),
        "top_p": settings.value("ModelTopP", 0.95, type=float),
        "top_k": settings.value("ModelTopK", 40, type=int),
        "max_output_tokens": 8192,
        "response_mime_type": "text/plain",
    }


SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
}


@dataclass
class ModelConfig:
    MODEL_NAME: str
    BATCH_SIZE: int
    GENERATION_CONFIG: Dict[str, Any]
    SAFETY_SETTINGS: Dict[HarmCategory, HarmBlockThreshold]


GEMINI_FLASH_MODEL_CONFIG = ModelConfig("gemini-2.0-flash", 15, get_generation_config(), SAFETY_SETTINGS)
GEMINI_FLASH_LITE_MODEL_CONFIG = ModelConfig("gemini-2.0-flash-lite", 15, get_generation_config(), SAFETY_SETTINGS)
GEMINI_PRO_MODEL_CONFIG = ModelConfig("gemini-2.0-pro-exp-02-05", 2, get_generation_config(), SAFETY_SETTINGS)
GEMINI_FLASH_THINKING_MODEL_CONFIG = ModelConfig("gemini-2.0-flash-thinking-exp-01-21", 10, get_generation_config(), SAFETY_SETTINGS)


MODEL_CONFIGS = {
    "gemini-2.0-flash": GEMINI_FLASH_MODEL_CONFIG,
    "gemini-2.0-flash-lite": GEMINI_FLASH_LITE_MODEL_CONFIG,
    "gemini-2.0-pro": GEMINI_PRO_MODEL_CONFIG,
    "gemini-2.0-flash-thinking": GEMINI_FLASH_THINKING_MODEL_CONFIG,
}

def get_model_config(model_name: str) -> ModelConfig:
    """Get model configuration for the specified model with current settings."""
    return MODEL_CONFIGS.get(
        model_name, 
        GEMINI_FLASH_LITE_MODEL_CONFIG,
    )

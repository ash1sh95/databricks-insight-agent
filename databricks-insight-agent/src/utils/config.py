import os
from typing import Dict, Any
from pathlib import Path
import structlog

logger = structlog.get_logger(__name__)

class Config:
    """Centralized configuration management for the Databricks Insight Agent."""

    # Default configuration
    DEFAULTS = {
        # Databricks
        'DATABRICKS_HOST': '',
        'DATABRICKS_TOKEN': '',
        'DATABRICKS_WAREHOUSE_ID': '',

        # Model Serving
        'DATABRICKS_MODEL_SERVING_ENDPOINT': '',
        'DATABRICKS_MODEL_SERVING_TOKEN': '',

        # MLflow
        'MLFLOW_TRACKING_URI': 'file:./mlruns',
        'MLFLOW_EXPERIMENT_NAME': 'databricks-insight-agent',

        # Email alerts
        'SMTP_SERVER': '',
        'SMTP_PORT': '587',
        'SMTP_USER': '',
        'SMTP_PASS': '',
        'ALERT_EMAILS': '',

        # Application
        'LOG_LEVEL': 'INFO',
        'REPORTS_DIR': 'reports',
        'MAX_WORKERS': '4',
        'ANALYSIS_TIMEOUT': '3600',  # seconds
        'USE_MOCK_DATA': 'false',  # Use mock data instead of system tables (for free edition)

        # Agent configurations
        'NETWORK_ANALYSIS_MODEL': 'gpt-4',
        'SECURITY_ANALYSIS_MODEL': 'gpt-4',
        'EVALUATION_MODEL': 'gpt-4',

        # DSPy
        'DSPY_TEACHER_MODEL': 'gpt-4',
        'DSPY_TEACHER_MODEL_KWARGS': '{"temperature": 0.0}',

        # Security
        'ENABLE_ENCRYPTION': 'true',
        'ENCRYPTION_KEY': '',

        # Performance
        'BATCH_SIZE': '1000',
        'CACHE_TTL': '3600',  # seconds
    }

    def __init__(self, env_file: str = '.env'):
        self._config = {}
        self._load_defaults()
        self._load_env_file(env_file)
        self._load_environment_variables()
        self._validate_config()

    def _load_defaults(self):
        """Load default configuration values."""
        self._config.update(self.DEFAULTS)

    def _load_env_file(self, env_file: str):
        """Load configuration from .env file."""
        env_path = Path(env_file)
        if env_path.exists():
            try:
                with open(env_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            key, value = line.split('=', 1)
                            self._config[key.strip()] = value.strip().strip('"\'')

                logger.info("Loaded configuration from .env file")
            except Exception as e:
                logger.warning("Failed to load .env file", error=str(e))
        else:
            logger.info("No .env file found, using defaults and environment variables")

    def _load_environment_variables(self):
        """Override with environment variables."""
        for key in self._config.keys():
            env_value = os.getenv(key)
            if env_value is not None:
                self._config[key] = env_value

    def _validate_config(self):
        """Validate critical configuration values."""
        required = []

        # Only require Databricks credentials if not using mock data
        if not self.get_bool('USE_MOCK_DATA'):
            required.extend(['DATABRICKS_HOST', 'DATABRICKS_TOKEN'])

        # Either OpenAI or Databricks Model Serving must be configured
        model_configs = ['OPENAI_API_KEY', 'DATABRICKS_MODEL_SERVING_ENDPOINT']
        has_model_config = any(self._config.get(key) for key in model_configs)

        if not has_model_config:
            required.extend(model_configs)

        missing = []
        for key in required:
            if not self._config.get(key):
                missing.append(key)

        if missing:
            logger.warning("Missing required configuration", missing=missing)
            # Don't fail, allow partial config for development

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, default)

    def set(self, key: str, value: Any):
        """Set configuration value."""
        self._config[key] = value

    def all(self) -> Dict[str, Any]:
        """Get all configuration values."""
        return self._config.copy()

    def is_production(self) -> bool:
        """Check if running in production mode."""
        return os.getenv('ENVIRONMENT', 'development').lower() == 'production'

    def get_int(self, key: str, default: int = 0) -> int:
        """Get integer configuration value."""
        try:
            return int(self.get(key, default))
        except (ValueError, TypeError):
            return default

    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean configuration value."""
        value = self.get(key, '').lower()
        return value in ('true', '1', 'yes', 'on')

    def get_list(self, key: str, default: list = None) -> list:
        """Get list configuration value (comma-separated)."""
        if default is None:
            default = []

        value = self.get(key, '')
        if not value:
            return default

        return [item.strip() for item in value.split(',') if item.strip()]

# Global configuration instance
config = Config()
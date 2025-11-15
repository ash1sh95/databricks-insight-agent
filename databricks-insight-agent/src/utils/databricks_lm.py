"""
Custom DSPy Language Model for Databricks Model Serving endpoints.
"""

import requests
import json
from typing import List, Dict, Any, Optional
import dspy
from .config import config
import structlog

logger = structlog.get_logger(__name__)

class DatabricksLM(dspy.LM):
    """
    DSPy Language Model implementation for Databricks Model Serving.

    Supports both Foundation Model APIs and custom model serving endpoints.
    """

    def __init__(
        self,
        model: str = "databricks-meta-llama-3-1-70b-instruct",
        endpoint_url: Optional[str] = None,
        api_key: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 1000,
        **kwargs
    ):
        super().__init__(model)

        self.endpoint_url = endpoint_url or config.get('DATABRICKS_MODEL_SERVING_ENDPOINT')
        self.api_key = api_key or config.get('DATABRICKS_MODEL_SERVING_TOKEN') or config.get('DATABRICKS_TOKEN')
        self.temperature = temperature
        self.max_tokens = max_tokens

        if not self.endpoint_url:
            raise ValueError("Databricks Model Serving endpoint URL is required")

        if not self.api_key:
            raise ValueError("Databricks API key is required")

        # Set model-specific parameters
        self.kwargs = {
            "temperature": temperature,
            "max_tokens": max_tokens,
            **kwargs
        }

    def __call__(
        self,
        prompt: str = None,
        messages: List[Dict[str, Any]] = None,
        **kwargs
    ) -> List[str]:
        """
        Generate completions for the given prompt or messages.

        Args:
            prompt: Single prompt string
            messages: List of message dictionaries
            **kwargs: Additional parameters

        Returns:
            List of completion strings
        """
        # Prepare the request payload
        if messages:
            # Convert messages to payload format
            payload = self._prepare_messages_payload(messages, **kwargs)
        elif prompt:
            # Convert single prompt to messages format
            messages = [{"role": "user", "content": prompt}]
            payload = self._prepare_messages_payload(messages, **kwargs)
        else:
            raise ValueError("Either prompt or messages must be provided")

        # Make the API call
        response = self._call_endpoint(payload)

        # Extract completions
        completions = self._extract_completions(response)

        return completions

    def _prepare_messages_payload(
        self,
        messages: List[Dict[str, Any]],
        **kwargs
    ) -> Dict[str, Any]:
        """Prepare the payload for messages-based requests."""
        # Merge instance kwargs with call kwargs
        call_kwargs = {**self.kwargs, **kwargs}

        payload = {
            "messages": messages,
            "max_tokens": call_kwargs.get("max_tokens", self.max_tokens),
            "temperature": call_kwargs.get("temperature", self.temperature),
        }

        # Add optional parameters
        if "top_p" in call_kwargs:
            payload["top_p"] = call_kwargs["top_p"]
        if "top_k" in call_kwargs:
            payload["top_k"] = call_kwargs["top_k"]
        if "stop" in call_kwargs:
            payload["stop"] = call_kwargs["stop"]

        return payload

    def _call_endpoint(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make the API call to Databricks Model Serving."""
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        try:
            logger.debug("Calling Databricks Model Serving", endpoint=self.endpoint_url)

            response = requests.post(
                self.endpoint_url,
                headers=headers,
                json=payload,
                timeout=60  # 60 second timeout
            )

            response.raise_for_status()

            result = response.json()
            logger.debug("Model serving response received")

            return result

        except requests.exceptions.RequestException as e:
            logger.error("Failed to call Databricks Model Serving", error=str(e))
            raise dspy.DSPyException(f"Model serving call failed: {str(e)}")

    def _extract_completions(self, response: Dict[str, Any]) -> List[str]:
        """Extract completion text from the API response."""
        try:
            # Handle different response formats
            if "choices" in response:
                # OpenAI-compatible format
                completions = []
                for choice in response["choices"]:
                    if "message" in choice and "content" in choice["message"]:
                        completions.append(choice["message"]["content"])
                    elif "text" in choice:
                        completions.append(choice["text"])
                return completions

            elif "predictions" in response:
                # Databricks predictions format
                predictions = response["predictions"]
                if isinstance(predictions, list):
                    return [str(pred) for pred in predictions]
                else:
                    return [str(predictions)]

            elif "content" in response:
                # Simple content format
                return [response["content"]]

            else:
                # Try to extract any text content
                text_content = json.dumps(response)
                logger.warning("Unexpected response format, using raw JSON")
                return [text_content]

        except Exception as e:
            logger.error("Failed to extract completions", error=str(e))
            raise dspy.DSPyException(f"Failed to parse response: {str(e)}")

    @property
    def max_tokens(self) -> int:
        """Maximum tokens for generation."""
        return self.kwargs.get("max_tokens", 1000)

    @max_tokens.setter
    def max_tokens(self, value: int):
        self.kwargs["max_tokens"] = value

def create_databricks_lm(model_name: str = "databricks-meta-llama-3-1-70b-instruct") -> DatabricksLM:
    """
    Factory function to create a Databricks LM instance.

    Args:
        model_name: Name of the model to use

    Returns:
        Configured DatabricksLM instance
    """
    return DatabricksLM(model=model_name)

def setup_dspy_with_databricks():
    """
    Configure DSPy to use Databricks Model Serving as the default LM.
    """
    lm = create_databricks_lm()
    dspy.settings.configure(lm=lm)
    logger.info("DSPy configured with Databricks Model Serving")
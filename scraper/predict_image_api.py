import os
from typing import Optional

import requests


class PredictImageAPI:
    """HTTP client for the OCR model service.

    The TensorFlow model is hosted in a separate container (ai-model).
    """

    def __init__(
        self,
        model_path: Optional[str] = None,
        *,
        base_url: Optional[str] = None,
        timeout_seconds: int = 15,
    ):
        self.base_url = (base_url or os.getenv("AI_MODEL_URL") or "").rstrip("/")
        self.timeout_seconds = timeout_seconds

        if not self.base_url:
            raise RuntimeError(
                "AI_MODEL_URL is not set. Start docker compose (ai-model service) or set AI_MODEL_URL."
            )

    def warmup(self) -> None:
        try:
            requests.get(f"{self.base_url}/health", timeout=self.timeout_seconds)
        except Exception:
            return None

    def predict_image(self, image_path: str) -> str:
        with open(image_path, "rb") as f:
            files = {"file": ("captcha.png", f, "image/png")}
            r = requests.post(
                f"{self.base_url}/predict",
                files=files,
                timeout=self.timeout_seconds,
            )
        r.raise_for_status()
        data = r.json() if r.content else {}
        return str(data.get("text") or "")

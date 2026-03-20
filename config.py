# -*- coding: utf-8 -*-
"""
Application configuration — validated via pydantic-settings.
Catches missing env vars at startup and configures logging.
"""

import base64
import json
import logging
import sys

from pydantic_settings import BaseSettings
from pydantic import model_validator


class Settings(BaseSettings):
    """All config is loaded from the .env file (or real env vars)."""

    # Required
    STREMIO_USER: str
    STREMIO_PASS: str
    PTUBE_MANIFEST: str
    RD_TOKEN: str
    STASHDB_API_KEY: str

    # Optional with defaults
    DEBUG_MODE: bool = True
    PORT: int = 9000

    # Derived (set by validator)
    PTUBE_BASE: str = ""
    PTUBE_FALLBACK_BASE: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @model_validator(mode="after")
    def derive_ptube_urls(self):
        """Compute PTUBE_BASE and PTUBE_FALLBACK_BASE from the manifest URL."""
        self.PTUBE_BASE = self.PTUBE_MANIFEST.rsplit("/manifest.json", 1)[0]

        try:
            url_parts = self.PTUBE_BASE.split("/")
            config_b64 = url_parts[-1]
            host = "/".join(url_parts[:-1])

            pad = len(config_b64) % 4
            b64_str = config_b64 + "=" * ((4 - pad) % 4)
            if "-" in b64_str or "_" in b64_str:
                config_json = base64.urlsafe_b64decode(b64_str).decode("utf-8")
            else:
                config_json = base64.b64decode(b64_str).decode("utf-8")

            config = json.loads(config_json)
            config["hideTorrents"] = False

            new_json_bytes = json.dumps(config).encode("utf-8")
            new_b64 = base64.urlsafe_b64encode(new_json_bytes).decode("utf-8").rstrip("=")
            self.PTUBE_FALLBACK_BASE = f"{host}/{new_b64}"
        except Exception:
            self.PTUBE_FALLBACK_BASE = self.PTUBE_BASE

        return self


def setup_logging(debug: bool) -> None:
    """Configure the root logger based on DEBUG_MODE."""
    level = logging.DEBUG if debug else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    datefmt = "%H:%M:%S"

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        stream=sys.stdout,
        force=True,
    )
    # Quiet noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Singleton — import ``settings`` everywhere
# ---------------------------------------------------------------------------
settings = Settings()
setup_logging(settings.DEBUG_MODE)

log = logging.getLogger("heremio")

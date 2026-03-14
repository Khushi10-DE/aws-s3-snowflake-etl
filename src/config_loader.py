"""
config_loader.py
----------------
Loads YAML config and merges with environment variables.
Never hardcode credentials — always use .env or environment variables.

Author: Khushbu Gohil
"""

import os
import logging

import yaml
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)


def load_config(path: str = "config/config.yaml") -> dict:
    """
    Load config YAML and override sensitive values from environment variables.
    Environment variables always take precedence over the YAML file.
    """
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # AWS credentials from env
    config["aws"]["aws_access_key_id"] = os.getenv(
        "AWS_ACCESS_KEY_ID", config["aws"].get("aws_access_key_id", "")
    )
    config["aws"]["aws_secret_access_key"] = os.getenv(
        "AWS_SECRET_ACCESS_KEY", config["aws"].get("aws_secret_access_key", "")
    )

    # Snowflake credentials from env
    config["snowflake"]["user"] = os.getenv(
        "SNOWFLAKE_USER", config["snowflake"].get("user", "")
    )
    config["snowflake"]["password"] = os.getenv(
        "SNOWFLAKE_PASSWORD", config["snowflake"].get("password", "")
    )
    config["snowflake"]["account"] = os.getenv(
        "SNOWFLAKE_ACCOUNT", config["snowflake"].get("account", "")
    )

    log.info(f"Config loaded from {path}")
    return config

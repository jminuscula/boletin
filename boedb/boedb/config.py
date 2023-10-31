import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values

CONFIG_BASE_PATH = Path(os.path.abspath(os.path.dirname(__file__))) / Path("..")

config = {
    **dotenv_values(CONFIG_BASE_PATH / Path(".env.development")),  # common configurable settings
    **dotenv_values(CONFIG_BASE_PATH / Path(".env.secret")),  # API keys
    **os.environ,  # environment overrides
}


LOGGERS = {}


def get_logger(name="boedb", level=None):
    if name in LOGGERS:
        return LOGGERS[name]

    format = "{asctime}.{msecs:03.0f} - {name} - {levelname} - {msg}"
    root_level = level or int(config.get("LOG_LEVEL", logging.INFO))
    app_level = level or int(config.get("LOG_LEVEL_APP", root_level))
    datefmt = r"%Y-%m-%dT%H:%M:%S"

    # configure root and third party loggers
    logging.basicConfig(format=format, level=root_level, datefmt=datefmt, style="{")

    handler = logging.StreamHandler()
    formatter = logging.Formatter(format, datefmt, style="{")
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(app_level)
    logger.propagate = False
    LOGGERS[name] = logger

    return logger


@dataclass
class DiarioBoeConfig:
    # Number of concurrent requests to extract Article data from boe.es
    ARTICLE_EXTRACT_BATCH_SIZE = 10

    # Number of articles to be processed concurrently, including
    # LLM processing through OpenAi's API
    ARTICLE_TRANSFORM_BATCH_SIZE = 10

    # We can't exceed LLM's max context tokens, so the original text
    # plus the generated outcome must be controlled
    ARTICLE_FRAGMENT_MAX_LENGTH = 8192


@dataclass
class DBConfig:
    USER = config["PG_USER"]
    PASSWORD = config["PG_PASSWORD"]
    DBNAME = config["PG_DBNAME"]
    DSN = f"user={USER} password={PASSWORD} dbname={DBNAME}"


@dataclass
class OpenAiConfig:
    API_KEY = config["OPENAI_API_KEY"]

import os
from dataclasses import dataclass

from dotenv import dotenv_values

config = {
    **dotenv_values(".env.development"),  # common configurable settings
    **dotenv_values(".env.secret"),  # API keys
    **os.environ,  # environment overrides
}


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

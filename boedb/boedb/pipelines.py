import asyncio
from datetime import datetime

from boedb.client import get_http_client_session
from boedb.config import DBConfig, DiarioBoeConfig
from boedb.diario_boe.extract import (
    DiarioBoeArticlesExtractor,
    DiarioBoeSummaryExtractor,
)
from boedb.diario_boe.load import DiarioBoeArticlesLoader, DiarioBoeSummaryLoader
from boedb.diario_boe.transform import DiarioBoeArticleTransformer


class Pipeline:
    """
    Orchestrates the ETL pipeline.
    The extractor, transformer and loader callables are asynchronous functions responsible
    for implementing their own concurrency mechanisms.

    :param extractor: async callable to produce items
    :param transformer: async callable to transform extracted items
    :param loader: async callable to load transformed items
    """

    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    async def run(self):
        items = await self.extractor()
        if self.transformer:
            items = await self.transformer(items)
        if self.loader:
            items = await self.loader(items)
        return items


async def process_diario_boe_for_date(date):
    async with get_http_client_session() as http_session:
        summary = await Pipeline(
            extractor=DiarioBoeSummaryExtractor(date, http_session),
            transformer=None,
            loader=DiarioBoeSummaryLoader(DBConfig.DSN),
        ).run()

        articles = await Pipeline(
            extractor=DiarioBoeArticlesExtractor(
                summary,
                http_session,
                batch_size=DiarioBoeConfig.ARTICLE_EXTRACT_BATCH_SIZE,
            ),
            transformer=DiarioBoeArticleTransformer(
                http_session, batch_size=DiarioBoeConfig.ARTICLE_TRANSFORM_BATCH_SIZE
            ),
            loader=DiarioBoeArticlesLoader(DBConfig.DSN),
        ).run()


async def test_diario_boe_article_process():  # pragma: no cover
    from diario_boe.models import DaySummary, DaySummaryEntry

    entry = DaySummaryEntry("BOE-S-20231023", "BOE-A-2023-21730")
    summary = DaySummary("BOE-S-20231023", {"fecha": "23/10/2023"}, [entry])

    async with get_http_client_session() as http_session:
        articles = await Pipeline(
            extractor=DiarioBoeArticlesExtractor(summary, http_session, batch_size=1),
            transformer=DiarioBoeArticleTransformer(http_session, batch_size=1),
            # loader=DiarioBoeArticlesLoader(DBConfig.DSN),
            loader=None,
        ).run()

    article = articles[0]
    print(f"Article: {article.article_id}")
    print(f"Title: {article.title}")
    print(f"Summary: {article.summary[:256]}")


if __name__ == "__main__":
    asyncio.run(test_diario_boe_article_process())

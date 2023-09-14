import asyncio
import contextlib
import itertools
from datetime import datetime

from boedb.client import get_http_client_session
from boedb.diario_boe.extract import (
    DiarioBoeArticlesExtractor,
    DiarioBoeSummaryExtractor,
)


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
            loader=None,
        ).run()

        articles = await Pipeline(
            extractor=DiarioBoeArticlesExtractor(summary, http_session),
            transformer=None,
            loader=None,
        ).run()

        for article in articles:
            print(article)


if __name__ == "__main__":
    date = datetime(2023, 9, 8)
    asyncio.run(process_diario_boe_for_date(date), debug=True)

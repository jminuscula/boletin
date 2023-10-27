import asyncio

from boedb.client import get_http_client_session
from boedb.config import DBConfig, DiarioBoeConfig, get_logger
from boedb.diario_boe.extract import DiarioBoeArticlesExtractor, DiarioBoeSummaryExtractor
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
    logger = get_logger()
    logger.info(f"Starting Pipeline for date {date.isoformat()}")

    async with get_http_client_session() as http_session:
        summary = await Pipeline(
            extractor=DiarioBoeSummaryExtractor(date, http_session),
            transformer=None,
            loader=DiarioBoeSummaryLoader(DBConfig.DSN),
        ).run()

        logger.info(f"Processed summary {summary.summary_id}")

        fragments = await Pipeline(
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

        article_ids = set(f.article_id for f in fragments)
        logger.info(f"{len(article_ids)} articles, {len(fragments)} fragments processed")


async def test_diario_boe_article_process():  # pragma: no cover
    from diario_boe.models import DaySummary, DaySummaryEntry

    logger = get_logger("boedb.test_pipeline")
    entry = DaySummaryEntry("BOE-S-20231023", "BOE-A-2023-21737")
    summary = DaySummary("BOE-S-20231023", {"fecha": "23/10/2023"}, [entry])

    logger.info("Starting Pipeline")
    async with get_http_client_session() as http_session:
        fragments = await Pipeline(
            extractor=DiarioBoeArticlesExtractor(summary, http_session, batch_size=1),
            # transformer=DiarioBoeArticleTransformer(http_session, batch_size=10),
            # loader=DiarioBoeArticlesLoader(DBConfig.DSN),
            transformer=None,
            loader=None,
        ).run()

    article_ids = set(f.article_id for f in fragments)
    logger.info(f"{len(article_ids)} articles, {len(fragments)} fragments processed")


if __name__ == "__main__":
    asyncio.run(test_diario_boe_article_process())

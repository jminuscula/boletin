import asyncio
from datetime import datetime, timedelta

from boedb.client import get_http_client_session
from boedb.config import DBConfig, DiarioBoeConfig, get_logger
from boedb.diario_boe.extract import DiarioBoeArticlesStreamExtractor, DiarioBoeSummaryExtractor
from boedb.diario_boe.load import DiarioBoeArticlesLoader, DiarioBoeSummaryLoader
from boedb.diario_boe.transform import DiarioBoeArticleTransformer
from boedb.pipelines import StepPipeline


async def process_diario_boe_for_date(date):
    logger = get_logger()
    logger.info(f"Starting Pipeline for date {date.isoformat()}")

    async with get_http_client_session() as http_session:
        summary = await StepPipeline(
            extractor=DiarioBoeSummaryExtractor(date, http_session),
            transformer=None,
            loader=DiarioBoeSummaryLoader(DBConfig.DSN),
        ).run()

        logger.info(f"Processed summary {summary.summary_id}")

        fragments = await StepPipeline(
            extractor=DiarioBoeArticlesStreamExtractor(
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


if __name__ == "__main__":
    target_date = datetime.today() - timedelta(days=1)
    while target_date >= datetime(2023, 10, 28):
        asyncio.run(process_diario_boe_for_date(target_date))
        target_date -= timedelta(days=1)

import asyncio
from datetime import datetime, timedelta

from boedb.client import HttpClient, get_http_client_session
from boedb.config import get_logger
from boedb.diario_boe.models import DocumentError
from boedb.diario_boe.pipelines import DiarioBoeArticlesPipeline, DiarioBoeSummaryPipeline


async def process_diario_boe_for_date(date):
    logger = get_logger()
    logger.info(f"Starting Pipeline for date {date.isoformat()}")

    async with get_http_client_session() as http_session:
        summary_pipeline = DiarioBoeSummaryPipeline(date, http_session)
        try:
            summary = await summary_pipeline.run()
        except DocumentError:
            logger.warning(f"Summary for {date} not found")
            return

        # summary might have been skipped
        if summary is None:
            return

        logger.info(f"Processed summary {summary.summary_id} ({len(summary.items)} entries)")

        processed = 0
        article_ids = set()
        articles_pipeline = DiarioBoeArticlesPipeline(http_session)
        async for item in articles_pipeline.run(summary.items):
            processed += 1
            article_ids.add(item.article_id)

        logger.info(f"{len(article_ids)} articles, {processed} items processed")


if __name__ == "__main__":
    target_date = datetime(2023, 11, 2)
    while target_date >= datetime(2023, 11, 1):
        asyncio.run(process_diario_boe_for_date(target_date))
        target_date -= timedelta(days=1)

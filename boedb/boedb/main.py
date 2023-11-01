import asyncio
from datetime import datetime, timedelta

from boedb.client import get_http_client_session
from boedb.config import get_logger
from boedb.diario_boe.pipelines import DiarioBoeArticlesPipeline, DiarioBoeSummaryPipeline


async def process_diario_boe_for_date(date):
    logger = get_logger()
    logger.info(f"Starting Pipeline for date {date.isoformat()}")

    async with get_http_client_session() as http_session:
        summary_pipeline = DiarioBoeSummaryPipeline(date, http_session)
        summary = await summary_pipeline.run()
        logger.info(f"Processed summary {summary.summary_id} ({len(summary.items)} entries)")

        fragments_pipeline = DiarioBoeArticlesPipeline(http_session)
        processed = await fragments_pipeline.run_and_collect(summary.items)

        article_ids = set(f.article_id for f in processed)
        logger.info(f"{len(article_ids)} articles, {len(processed)} fragments processed")


if __name__ == "__main__":
    target_date = datetime.today() - timedelta(days=1)
    while target_date >= datetime(2023, 10, 31):
        asyncio.run(process_diario_boe_for_date(target_date))
        target_date -= timedelta(days=1)

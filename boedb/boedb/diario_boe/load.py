from boedb.config import get_logger
from boedb.db import get_db_client
from boedb.diario_boe.models import Article, ArticleFragment
from boedb.pipelines.step import BaseStepLoader
from boedb.pipelines.stream import StreamPipelineBaseExecutor


class SummaryLoader(BaseStepLoader):
    def __init__(self, should_skip=None):
        self.should_skip = should_skip
        self.db_client = get_db_client()
        self.logger = get_logger("boedb.diario_boe.summary_loader")

    async def __call__(self, summary):
        if self.should_skip is not None and self.should_skip(summary):
            self.logger.info(f"Skipped loading of {summary}")
            return summary

        columns = ("summary_id", "pubdate", "metadata", "n_articles")
        self.db_client.insert("es_diario_boe_summary", summary.as_dict(), columns)

        self.logger.debug(f"Loaded summary {summary.summary_id}")
        return summary


class ArticlesLoader(StreamPipelineBaseExecutor):
    def __init__(self, concurrency=1):
        self.logger = get_logger("boedb.diario_boe.summary_loader")
        super().__init__(concurrency)

        self.article_cols = (
            "article_id",
            "summary_id",
            "pubdate",
            "metadata",
            "title",
            "title_summary",
            "title_embedding",
            "n_fragments",
        )

        self.fragment_cols = (
            "article_id",
            "sequence",
            "content",
            "summary",
            "embedding",
        )

        self.db_client = get_db_client()

    async def process(self, item):
        if isinstance(item, Article):
            await self.load_article(item.as_dict())
            self.logger.debug(f"Loaded article {item}")

        elif isinstance(item, ArticleFragment):
            await self.load_fragment(item.as_dict())
            self.logger.debug(f"Loaded fragment {item}")

        return item

    async def load_article(self, row_dict):
        return self.db_client.insert("es_diario_boe_article", row_dict, self.article_cols)

    async def load_fragment(self, row_dict):
        return self.db_client.insert("es_diario_boe_article_fragment", row_dict, self.fragment_cols)

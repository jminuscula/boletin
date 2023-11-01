from boedb.config import get_logger
from boedb.loaders.db import PostgresDocumentLoader
from boedb.pipelines.step import BaseStepLoader
from boedb.pipelines.stream import StreamPipelineBaseExecutor


class SummaryLoader(PostgresDocumentLoader, BaseStepLoader):
    def __init__(self, dsn):
        columns = ("summary_id", "pubdate", "metadata")
        self.logger = get_logger("boedb.diario_boe.summary_loader")
        super().__init__(dsn, "es_diario_boe_summary", columns)

    async def __call__(self, summary):
        await super().__call__([summary.as_dict()])

        self.logger.debug(f"Loaded summary {summary.summary_id}")
        return summary


class ArticlesLoader(StreamPipelineBaseExecutor):
    def __init__(self, concurrency, dsn):
        self.logger = get_logger("boedb.diario_boe.summary_loader")
        super().__init__(concurrency)

        article_cols = (
            "article_id",
            "summary_id",
            "pubdate",
            "metadata",
            "title",
            "title_summary",
            "title_embedding",
        )

        fragment_cols = (
            "article_id",
            "sequence",
            "content",
            "summary",
            "embedding",
        )

        self.loaded_articles = set()
        self.article_loader = PostgresDocumentLoader(dsn, "es_diario_boe_article", article_cols)
        self.fragment_loader = PostgresDocumentLoader(dsn, "es_diario_boe_article_fragment", fragment_cols)

    async def process(self, item):
        if item.article_id not in self.loaded_articles:
            self.loaded_articles.add(item.article_id)

            article = item.as_article_dict()
            await self.article_loader(article)

            self.logger.debug(f"Loaded article {item}")

        fragment = item.as_fragment_dict()
        await self.fragment_loader(fragment)
        self.logger.debug(f"Loaded fragment {item}")

        return item

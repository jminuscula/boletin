from boedb.config import get_logger
from boedb.loaders.db import PostgresDocumentLoader


class DiarioBoeSummaryLoader(PostgresDocumentLoader):
    def __init__(self, dsn):
        columns = ("summary_id", "pubdate", "metadata")
        super().__init__(dsn, "es_diario_boe_summary", columns)


class DiarioBoeArticlesLoader:
    def __init__(self, dsn):
        article_cols = (
            "article_id",
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

        self.article_loader = PostgresDocumentLoader(dsn, "es_diario_boe_article", article_cols)
        self.fragment_loader = PostgresDocumentLoader(dsn, "es_diario_boe_article_fragment", fragment_cols)
        self.logger = get_logger("boedb.diario_boe.articles_loader")

    async def __call__(self, objs):
        articles = []
        fragments = []
        loaded_article_ids = set()

        for obj in objs:
            fragments.append(obj.as_fragment_dict())
            if obj.article_id not in loaded_article_ids:
                articles.append(obj.as_article_dict())
                loaded_article_ids.add(obj.article_id)

        await self.article_loader(articles)
        self.logger.debug(f"Loaded {len(articles)} articles")

        await self.fragment_loader(fragments)
        self.logger.debug(f"Loaded {len(fragments)} fragments")

        return objs

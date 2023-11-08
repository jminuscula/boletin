from boedb.config import DBConfig, DiarioBoeConfig
from boedb.db import get_db_client
from boedb.diario_boe.extract import ArticlesExtractor, SummaryExtractor
from boedb.diario_boe.load import ArticlesLoader, SummaryLoader
from boedb.diario_boe.transform import ArticlesTransformer
from boedb.pipelines.step import StepPipeline
from boedb.pipelines.stream import StreamPipeline


class DiarioBoeSummaryPipeline(StepPipeline):
    def __init__(self, date, http_session):
        self.db_client = get_db_client()

        extractor = SummaryExtractor(date, http_session, should_skip=self.get_extract_filter())
        transformer = None
        loader = SummaryLoader(should_skip=self.get_load_filter())

        super().__init__(extractor, transformer, loader)

    def get_extract_filter(self):
        sql = """
            select
                summary_id
            from
                es_diario_boe_summary
            where
                summary_id not in (select summary_id from es_diario_boe_summary_incomplete)
        """

        summary_ids = set([row[0] for row in self.db_client.execute(sql)])

        def should_skip(summary):
            return summary.summary_id in summary_ids

        return should_skip

    def get_load_filter(self):
        sql = """
            select
                summary_id
            from
                es_diario_boe_summary
        """

        summary_ids = set([row[0] for row in self.db_client.execute(sql)])

        def should_skip(summary):
            return summary.summary_id in summary_ids

        return should_skip


class DiarioBoeArticlesPipeline(StreamPipeline):
    def __init__(self, http_session):
        self.db_client = get_db_client()

        extractor = ArticlesExtractor(
            DiarioBoeConfig.ARTICLE_EXTRACT_CONCURRENCY,
            http_session,
            should_skip=self.get_extract_filter(),
        )
        transformer = ArticlesTransformer(DiarioBoeConfig.ARTICLE_TRANSFORM_CONCURRENCY, http_session)
        loader = ArticlesLoader(DiarioBoeConfig.ARTICLE_LOAD_CONCURRENCY)

        super().__init__(extractor, transformer, loader)

    def get_extract_filter(self):
        sql = """
            select
                article_id
            from
                es_diario_boe_article
        """

        article_ids = set([row[0] for row in self.db_client.execute(sql)])

        def should_skip(item):
            return item.entry_id in article_ids

        return should_skip

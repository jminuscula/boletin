from boedb.config import DBConfig, DiarioBoeConfig
from boedb.diario_boe.extract import ArticlesExtractor, SummaryExtractor
from boedb.diario_boe.load import ArticlesLoader, SummaryLoader
from boedb.diario_boe.transform import ArticlesTransformer
from boedb.pipelines.step import StepPipeline
from boedb.pipelines.stream import StreamPipeline


class DiarioBoeSummaryPipeline(StepPipeline):
    def __init__(self, date, http_session):
        extractor = SummaryExtractor(date, http_session)
        transformer = None
        loader = SummaryLoader(DBConfig.DSN)

        super().__init__(extractor, transformer, loader)


class DiarioBoeArticlesPipeline(StreamPipeline):
    def __init__(self, http_session):
        extractor = ArticlesExtractor(DiarioBoeConfig.ARTICLE_TRANSFORM_CONCURRENCY, http_session)
        transformer = ArticlesTransformer(DiarioBoeConfig.ARTICLE_TRANSFORM_CONCURRENCY, http_session)
        loader = ArticlesLoader(DiarioBoeConfig.ARTICLE_LOAD_CONCURRENCY, DBConfig.DSN)

        super().__init__(extractor, transformer, loader)

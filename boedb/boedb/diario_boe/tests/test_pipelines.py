from datetime import datetime
from unittest import mock

from boedb.diario_boe.models import DaySummary, DaySummaryEntry
from boedb.diario_boe.pipelines import DiarioBoeArticlesPipeline, DiarioBoeSummaryPipeline


@mock.patch("boedb.diario_boe.pipelines.get_db_client")
@mock.patch("boedb.diario_boe.pipelines.SummaryLoader")
@mock.patch("boedb.diario_boe.pipelines.SummaryExtractor")
def test_summary_pipeline_inits_ok(extractor_mock, loader_mock, get_db_mock):
    date = datetime(2023, 11, 10)
    session = mock.Mock()

    extract_filter = "boedb.diario_boe.pipelines.DiarioBoeSummaryPipeline.get_extract_filter"
    load_filter = "boedb.diario_boe.pipelines.DiarioBoeSummaryPipeline.get_load_filter"
    with mock.patch(extract_filter) as ef_mock, mock.patch(load_filter) as lf_mock:
        pipeline = DiarioBoeSummaryPipeline(date, session)

    assert pipeline.db_client == get_db_mock.return_value
    extractor_mock.assert_called_once_with(date, session, should_skip=ef_mock.return_value)
    loader_mock.assert_called_once_with(should_skip=lf_mock.return_value)


@mock.patch("boedb.diario_boe.pipelines.get_db_client")
@mock.patch("boedb.diario_boe.pipelines.SummaryExtractor", mock.Mock())
@mock.patch("boedb.diario_boe.pipelines.SummaryLoader", mock.Mock())
def test_summary_pipeline_extract_should_skip(get_db_mock):
    date = datetime(2023, 11, 10)
    session = mock.Mock()
    summary_rows = [["summary-1"], ["summary-2"]]

    get_db_mock.return_value.execute.return_value = summary_rows
    pipeline = DiarioBoeSummaryPipeline(date, session)
    should_skip = pipeline.get_extract_filter()

    assert should_skip(DaySummary("summary-1", {"fecha": "10/11/2023"})) is True
    assert should_skip(DaySummary("summary-2", {"fecha": "10/11/2023"})) is True
    assert should_skip(DaySummary("summary-3", {"fecha": "10/11/2023"})) is False


@mock.patch("boedb.diario_boe.pipelines.get_db_client")
@mock.patch("boedb.diario_boe.pipelines.SummaryLoader", mock.Mock())
def test_summary_pipeline_load_should_skip(get_db_mock):
    date = datetime(2023, 11, 10)
    session = mock.Mock()
    summary_rows = [["summary-1"], ["summary-2"]]

    get_db_mock.return_value.execute.return_value = summary_rows
    pipeline = DiarioBoeSummaryPipeline(date, session)
    should_skip = pipeline.get_load_filter()

    assert should_skip(DaySummary("summary-1", {"fecha": "10/11/2023"})) is True
    assert should_skip(DaySummary("summary-2", {"fecha": "10/11/2023"})) is True
    assert should_skip(DaySummary("summary-3", {"fecha": "10/11/2023"})) is False


@mock.patch("boedb.diario_boe.pipelines.get_db_client")
@mock.patch("boedb.diario_boe.pipelines.ArticlesLoader")
@mock.patch("boedb.diario_boe.pipelines.ArticlesTransformer")
@mock.patch("boedb.diario_boe.pipelines.ArticlesExtractor")
@mock.patch("boedb.diario_boe.pipelines.DiarioBoeConfig.ARTICLE_EXTRACT_CONCURRENCY", 10)
@mock.patch("boedb.diario_boe.pipelines.DiarioBoeConfig.ARTICLE_TRANSFORM_CONCURRENCY", 20)
@mock.patch("boedb.diario_boe.pipelines.DiarioBoeConfig.ARTICLE_LOAD_CONCURRENCY", 30)
def test_articles_pipeline_inits_ok(extractor_mock, transformer_mock, loader_mock, get_db_mock):
    session = mock.Mock()
    extract_filter = "boedb.diario_boe.pipelines.DiarioBoeArticlesPipeline.get_extract_filter"
    with mock.patch(extract_filter) as ef_mock:
        pipeline = DiarioBoeArticlesPipeline(session)

    assert pipeline.db_client == get_db_mock.return_value
    extractor_mock.assert_called_once_with(10, session, should_skip=ef_mock.return_value)
    transformer_mock.assert_called_once_with(20, session)
    loader_mock.assert_called_once_with(30)


@mock.patch("boedb.diario_boe.pipelines.get_db_client")
@mock.patch("boedb.diario_boe.pipelines.ArticlesLoader", mock.Mock())
def test_articles_pipeline_extract_should_skip(get_db_mock):
    session = mock.Mock()
    summary_rows = [["article-1"], ["article-2"]]

    get_db_mock.return_value.execute.return_value = summary_rows
    pipeline = DiarioBoeArticlesPipeline(session)
    should_skip = pipeline.get_extract_filter()

    assert should_skip(DaySummaryEntry("summary-id", "article-1", {}, "title")) is True
    assert should_skip(DaySummaryEntry("summary-id", "article-2", {}, "title")) is True
    assert should_skip(DaySummaryEntry("summary-id", "article-3", {}, "title")) is False

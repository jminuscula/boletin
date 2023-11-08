from unittest import mock

import pytest

from boedb.diario_boe.load import ArticlesLoader, SummaryLoader
from boedb.diario_boe.models import Article, ArticleFragment, DaySummary


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.load.get_db_client")
async def test_summary_loader_loads_summary(get_db_client_mock):
    db_client_mock = mock.Mock()
    get_db_client_mock.return_value = db_client_mock
    serialized = mock.Mock()
    columns = ("summary_id", "pubdate", "metadata", "n_articles")
    with mock.patch.object(DaySummary, "as_dict", return_value=serialized):
        summary = DaySummary("BOE-S-20231023", {"fecha": "23/10/2023"})
        loader = SummaryLoader()
        await loader(summary)

    db_client_mock.insert.assert_called_once_with("es_diario_boe_summary", serialized, columns)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.load.get_db_client")
async def test_summary_loader_skips_summary(get_db_client_mock):
    db_client_mock = mock.Mock()
    get_db_client_mock.return_value = db_client_mock

    should_skip = mock.Mock(return_value=True)
    loader = SummaryLoader(should_skip=should_skip)
    summary = DaySummary("BOE-S-20231023", {"fecha": "23/10/2023"})
    result = await loader(summary)

    assert result is summary
    should_skip.assert_called_once_with(summary)
    db_client_mock.insert.assert_not_called()


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.load.get_db_client", mock.Mock())
async def test_articles_loader_processes_article_item():
    serialized = mock.Mock()
    article = Article("article-id", "summary-id", {"fecha_publicacion": "20231101"}, "content")

    loader = ArticlesLoader()
    with mock.patch.object(
        loader, "load_article", return_value=mock.AsyncMock()
    ) as load_article_mock, mock.patch.object(
        loader, "load_fragment", return_value=mock.AsyncMock()
    ) as load_fragment_mock, mock.patch.object(
        article, "as_dict", return_value=serialized
    ):
        await loader.process(article)

    load_article_mock.assert_awaited_once_with(serialized)
    load_fragment_mock.assert_not_awaited()


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.load.get_db_client", mock.Mock())
async def test_articles_loader_processes_fragment_item():
    serialized = mock.Mock()
    fragment = ArticleFragment("article-id", "content", 1, 10)

    loader = ArticlesLoader()
    with mock.patch.object(
        loader, "load_article", return_value=mock.AsyncMock()
    ) as load_article_mock, mock.patch.object(
        loader, "load_fragment", return_value=mock.AsyncMock()
    ) as load_fragment_mock, mock.patch.object(
        fragment, "as_dict", return_value=serialized
    ):
        await loader.process(fragment)

    load_fragment_mock.assert_awaited_once_with(serialized)
    load_article_mock.assert_not_awaited()


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.load.get_db_client")
async def test_articles_loader_loads_article(get_db_client_mock):
    db_client_mock = mock.Mock()
    get_db_client_mock.return_value = db_client_mock
    serialized = mock.Mock()
    columns = (
        "article_id",
        "summary_id",
        "pubdate",
        "metadata",
        "title",
        "title_summary",
        "title_embedding",
        "n_fragments",
    )
    with mock.patch.object(Article, "as_dict", return_value=serialized):
        article = Article("article-id", "summary-id", {"fecha_publicacion": "20231101"}, "content")
        loader = ArticlesLoader()
        await loader.load_article(article.as_dict())

    db_client_mock.insert.assert_called_once_with("es_diario_boe_article", serialized, columns)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.load.get_db_client")
async def test_articles_loader_loads_article_fragment(get_db_client_mock):
    db_client_mock = mock.Mock()
    get_db_client_mock.return_value = db_client_mock
    serialized = mock.Mock()
    columns = (
        "article_id",
        "sequence",
        "content",
        "summary",
        "embedding",
    )
    with mock.patch.object(ArticleFragment, "as_dict", return_value=serialized):
        fragment = ArticleFragment("article-id", "content", 1, 10)
        loader = ArticlesLoader()
        await loader.load_fragment(fragment.as_dict())

    db_client_mock.insert.assert_called_once_with("es_diario_boe_article_fragment", serialized, columns)

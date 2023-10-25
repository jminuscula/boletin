from datetime import datetime
from unittest import mock

import pytest

from boedb.diario_boe.load import DiarioBoeArticlesLoader
from boedb.diario_boe.models import Article


@pytest.mark.asyncio
async def test_diario_boe_article_loader_saves_article_once():
    articles = [
        Article("article-1", {"fecha_publicacion": "20231023"}, "content", 1, 2),
        Article("article-1", {"fecha_publicacion": "20231023"}, "content", 2, 2),
    ]

    article_dict = {
        "article_id": "article-1",
        "pubdate": datetime(2023, 10, 23),
        "metadata": '{"fecha_publicacion": "20231023"}',
        "title": None,
        "title_summary": None,
        "title_embedding": None,
    }

    article_loader_mock = mock.AsyncMock()
    fragment_loader_mock = mock.AsyncMock()
    with mock.patch(
        "boedb.diario_boe.load.PostgresDocumentLoader",
        side_effect=[article_loader_mock, fragment_loader_mock],
    ):
        loader = DiarioBoeArticlesLoader("test_dsn")
        await loader(articles)

    article_loader_mock.assert_awaited_once_with([article_dict])


@pytest.mark.asyncio
async def test_diario_boe_article_loader_saves_fragments():
    articles = [
        Article("article-1", {"fecha_publicacion": "20231023"}, "frag-1", 1, 2),
        Article("article-1", {"fecha_publicacion": "20231023"}, "frag-2", 2, 2),
    ]

    fragment_dicts = [
        {
            "article_id": "article-1",
            "sequence": 1,
            "content": "frag-1",
            "summary": None,
            "embedding": None,
        },
        {
            "article_id": "article-1",
            "sequence": 2,
            "content": "frag-2",
            "summary": None,
            "embedding": None,
        },
    ]

    article_loader_mock = mock.AsyncMock()
    fragment_loader_mock = mock.AsyncMock()
    with mock.patch(
        "boedb.diario_boe.load.PostgresDocumentLoader",
        side_effect=[article_loader_mock, fragment_loader_mock],
    ):
        loader = DiarioBoeArticlesLoader("test_dsn")
        await loader(articles)

    fragment_loader_mock.assert_awaited_once()
    fragment_loader_mock.assert_called_once_with(fragment_dicts)

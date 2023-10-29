from datetime import datetime
from unittest import mock
from xml.etree import ElementTree

import pytest
from aioresponses import aioresponses

from boedb.client import get_http_client_session
from boedb.diario_boe.extract import (
    DiarioBoeArticlesExtractor,
    DiarioBoeSummaryExtractor,
    extract_boe_article,
    extract_boe_summary,
    extract_boe_xml,
)
from boedb.diario_boe.models import Article, DaySummary


@pytest.mark.asyncio
async def test_extract_boe_xml_makes_request_and_returns_root():
    base_url = "https://www.boe.es/diario_boe/xml.php"
    xml_doc = b"<xml><test /></xml>"
    expected_url = f"{base_url}?id=doc_id"
    with aioresponses() as mock_server:
        mock_server.get(expected_url, status=200, body=xml_doc)
        async with get_http_client_session() as client:
            root = await extract_boe_xml("doc_id", client)
            mock_server.assert_called_once_with(
                base_url, **{"headers": {}, "trace_request_ctx": {"current_attempt": 1}}, params={"id": "doc_id"}
            )
            assert ElementTree.tostring(root) == xml_doc


@pytest.mark.asyncio
async def test_summary_extractor_extracts_doc_for_date():
    date = datetime(2023, 9, 14)
    summary_id = "BOE-S-20230914"
    mock_client = mock.Mock()
    extracted = mock.Mock()
    with mock.patch("boedb.diario_boe.extract.extract_boe_summary", return_value=extracted) as extract_mock:
        extractor = DiarioBoeSummaryExtractor(date, mock_client)

        result = await extractor()
        assert result is extracted
        extract_mock.assert_awaited_once_with(summary_id, mock_client)


@pytest.mark.asyncio
async def test_articles_extractor_extracts_items_from_summary():
    articles = [Article, Article]
    summary = mock.Mock(spec=DaySummary, items=articles)

    async with get_http_client_session() as client:
        extractor = DiarioBoeArticlesExtractor(summary, client)
        items = await extractor.gather()
        assert items == articles


@pytest.mark.asyncio
async def test_articles_extractor_extracts_items_in_process():
    articles = [Article, Article]
    summary = mock.Mock(spec=DaySummary, summary_id="summary_id", items=articles)
    item = mock.Mock(entry_id=1)
    extracted = mock.Mock()

    with mock.patch("boedb.diario_boe.extract.extract_boe_article", return_value=extracted) as extract_mock:
        async with get_http_client_session() as client:
            extractor = DiarioBoeArticlesExtractor(summary, client)
            processed = await extractor.process(item)

            extract_mock.assert_awaited_once_with(item.entry_id, summary.summary_id, client)
            assert processed == extracted


@pytest.mark.asyncio
async def test_articles_extractor_splits_articles():
    article = mock.Mock(spec=Article)
    subarticle = mock.Mock(spec=Article)()
    article.split.return_value = [subarticle, subarticle]
    summary = mock.Mock(spec=DaySummary, items=[article])

    async with get_http_client_session() as client:
        extractor = DiarioBoeArticlesExtractor(summary, client)
        extractor.process = mock.AsyncMock(side_effect=lambda i: i)

        result = await extractor()
        extractor.process.assert_called_with(article)
        assert result == [subarticle, subarticle]

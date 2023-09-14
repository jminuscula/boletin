from datetime import datetime
from unittest import mock

import pytest
from aioresponses import aioresponses

from boedb.client import get_http_client_session
from boedb.diario_boe.extract import (
    DiarioBoeArticlesExtractor,
    DiarioBoeSummaryExtractor,
    extract_boe_document,
)
from boedb.diario_boe.models import Article, DaySummary


@pytest.mark.asyncio
async def test_extract_boe_document_makes_request():
    url = "https://www.boe.es/diario_boe/xml.php?id=doc_id"
    with aioresponses() as mock_server:
        mock_server.get(url, status=200, body="test_doc")
        async with get_http_client_session() as client:
            with mock.patch("xmltodict.parse"):
                await extract_boe_document(mock.Mock(), "doc_id", client)
                mock_server.assert_called_once_with(url)


@pytest.mark.asyncio
async def test_extract_boe_document_inits_cls_from_xml_response():
    url = "https://www.boe.es/diario_boe/xml.php?id=doc_id"
    xml_response = "<xml>response</xml>"
    data = {"xml": "response"}
    test_cls = mock.Mock()
    with aioresponses() as mock_server:
        mock_server.get(url, status=200, body=xml_response)
        async with get_http_client_session() as client:
            with mock.patch("xmltodict.parse", return_value=data) as parse_mock:
                await extract_boe_document(test_cls, "doc_id", client)
                parse_mock.assert_called_once_with(xml_response)
                test_cls.from_dict.assert_called_once_with(data)


@pytest.mark.asyncio
async def test_summary_extractor_extracts_doc_for_date():
    date = datetime(2023, 9, 14)
    summary_id = "BOE-S-20230914"
    mock_client = mock.Mock()
    extracted = mock.Mock()
    with mock.patch("boedb.diario_boe.extract.extract_boe_document", return_value=extracted) as extract_mock:
        extractor = DiarioBoeSummaryExtractor(date, mock_client)

        result = await extractor()
        assert result is extracted
        extract_mock.assert_awaited_once_with(DaySummary, summary_id, mock_client)


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
    summary = mock.Mock(spec=DaySummary, items=articles)
    item = mock.Mock(entry_id=1)
    extracted = mock.Mock()

    with mock.patch("boedb.diario_boe.extract.extract_boe_document", return_value=extracted) as extract_mock:
        async with get_http_client_session() as client:
            extractor = DiarioBoeArticlesExtractor(summary, client)
            processed = await extractor.process(item)

            extract_mock.assert_awaited_once_with(Article, item.entry_id, client)
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

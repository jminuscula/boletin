from datetime import datetime
from unittest import mock
from xml.etree import ElementTree

import pytest
from aioresponses import aioresponses

from boedb.client import get_http_client_session
from boedb.diario_boe.extract import (
    ArticlesExtractor,
    SummaryExtractor,
    extract_boe_article,
    extract_boe_summary,
    extract_boe_xml,
)
from boedb.diario_boe.models import Article, ArticleFragment, DaySummary, DaySummaryEntry


@pytest.mark.asyncio
async def test_extract_boe_xml_makes_request_and_returns_root():
    url = "https://www.boe.es/diario_boe/xml.php?id=doc_id"
    xml_doc = b"<xml><test /></xml>"
    client_mock = mock.Mock()
    with mock.patch("boedb.diario_boe.extract.HttpClient") as HttpClientMock:
        HttpClientMock.return_value = client_mock
        client_mock.get = mock.AsyncMock(return_value=xml_doc)
        root = await extract_boe_xml("doc_id", client_mock)

    client_mock.get.assert_awaited_once_with(url, parse_response=False)
    assert ElementTree.tostring(root) == xml_doc


@pytest.mark.asyncio
async def test_extract_boe_summary_extracts_and_creates_daysummary():
    summary_id = "summary_id"
    root = mock.Mock()
    client_mock = mock.Mock()

    with mock.patch(
        "boedb.diario_boe.extract.extract_boe_xml", return_value=mock.AsyncMock()
    ) as extract_mock, mock.patch("boedb.diario_boe.extract.DaySummary") as DaySummaryMock:
        extract_mock.return_value = root

        await extract_boe_summary(summary_id, client_mock)

    extract_mock.assert_awaited_once_with(summary_id, client_mock)
    DaySummaryMock.from_xml.assert_called_once_with(root)


@pytest.mark.asyncio
async def test_extract_boe_article_extracts_and_creates_article():
    article_id, summary_id = "article_id", "summary_id"
    root = mock.Mock()
    client_mock = mock.Mock()

    with mock.patch(
        "boedb.diario_boe.extract.extract_boe_xml", return_value=mock.AsyncMock()
    ) as extract_mock, mock.patch("boedb.diario_boe.extract.Article") as ArticleMock:
        extract_mock.return_value = root

        await extract_boe_article(article_id, summary_id, client_mock)

    extract_mock.assert_awaited_once_with(article_id, client_mock)
    ArticleMock.from_xml.assert_called_once_with(root, summary_id)


@pytest.mark.asyncio
async def test_summary_extractor_extracts_doc_for_date():
    date = datetime(2023, 9, 14)
    summary_id = "BOE-S-20230914"
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()
    extracted = mock.Mock()
    with mock.patch(
        "boedb.diario_boe.extract.extract_boe_summary", return_value=extracted
    ) as extract_mock, mock.patch("boedb.diario_boe.extract.HttpClient") as HttpClientMock:
        HttpClientMock.return_value = client_mock
        extractor = SummaryExtractor(date, session_mock)

        result = await extractor()
        assert result is extracted

        extract_mock.assert_awaited_once_with(summary_id, client_mock)
        HttpClientMock.assert_called_once_with(session_mock)


@pytest.mark.asyncio
async def test_summary_extractor_skips_item_when_should_skip():
    date = datetime(2023, 9, 14)
    summary_id = "BOE-S-20230914"
    session_mock = mock.Mock()
    summary_mock = mock.Mock(spec=DaySummary, summary_id=summary_id)
    should_skip = mock.Mock(return_value=True)

    with mock.patch("boedb.diario_boe.extract.extract_boe_summary", return_value=summary_mock):
        extractor = SummaryExtractor(date, session_mock, should_skip)
        extracted = await extractor()

    should_skip.assert_called_once_with(summary_mock)
    assert extracted is None


@pytest.mark.asyncio
async def test_articles_extractor_process_extracts_items_from_entry():
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()

    summary_id, entry_id = "summary_id", "entry_id"
    entry = DaySummaryEntry(summary_id, entry_id)

    article_mock = mock.Mock(spec=Article)
    fragment_mock = mock.Mock(spec=ArticleFragment)
    article_mock.split.return_value = [fragment_mock]

    with mock.patch("boedb.diario_boe.extract.extract_boe_article") as extract_mock, mock.patch(
        "boedb.diario_boe.extract.HttpClient"
    ) as HttpClientMock:
        extract_mock.return_value = article_mock
        HttpClientMock.return_value = client_mock

        extractor = ArticlesExtractor(1, session_mock)
        result = await extractor.process(entry)

    extract_mock.assert_awaited_once_with(entry.entry_id, entry.summary_id, client_mock)
    assert result == [article_mock, fragment_mock]


@pytest.mark.asyncio
async def test_article_extractor_skips_item_when_should_skip():
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()

    summary_id, entry_id = "summary_id", "entry_id"
    entry = DaySummaryEntry(summary_id, entry_id)
    should_skip = mock.Mock(return_value=True)

    extractor = ArticlesExtractor(1, session_mock, should_skip)
    extracted = await extractor.process(entry)

    should_skip.assert_called_once_with(entry)
    assert extracted is None

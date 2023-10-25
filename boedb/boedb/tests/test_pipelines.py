from datetime import datetime
from unittest import mock

import pytest

from boedb.pipelines import Pipeline, process_diario_boe_for_date


@pytest.mark.asyncio
async def test_pipeline_runs_etl():
    item = mock.Mock()
    extractor = mock.AsyncMock(return_value=item)

    transformed = mock.Mock()
    transformer = mock.AsyncMock(return_value=transformed)

    loaded = mock.Mock()
    loader = mock.AsyncMock(return_value=loaded)

    pipeline = Pipeline(extractor, transformer, loader)
    processed = await pipeline.run()

    extractor.assert_called()
    transformer.assert_called_with(item)
    loader.assert_called_with(transformed)
    assert processed is loaded


@pytest.mark.asyncio
async def test_process_diario_boe_for_date_runs_pipeline():
    dt = datetime(2024, 10, 23)

    client_mock = mock.AsyncMock()
    get_client_mock = mock.AsyncMock()
    get_client_mock.__aenter__.return_value = client_mock

    summary_mock = mock.Mock()
    SumPipelineMock = mock.AsyncMock()
    SumPipelineMock.run.return_value = summary_mock
    ArtPipelineMock = mock.AsyncMock()

    with (
        mock.patch("boedb.pipelines.get_http_client_session", return_value=get_client_mock),
        mock.patch(
            "boedb.pipelines.DiarioBoeSummaryExtractor",
        ) as SummaryExtractorMock,
        mock.patch(
            "boedb.pipelines.DiarioBoeArticlesExtractor",
        ) as ArticlesExtractorMock,
        mock.patch("boedb.pipelines.Pipeline", side_effect=[SumPipelineMock, ArtPipelineMock]) as PipelineMock,
    ):
        await process_diario_boe_for_date(dt)

        SummaryExtractorMock.assert_called_once_with(dt, client_mock)
        SumPipelineMock.run.assert_awaited_once()

        ArticlesExtractorMock.assert_called_once_with(summary_mock, client_mock, batch_size=10)
        ArtPipelineMock.run.assert_awaited_once()

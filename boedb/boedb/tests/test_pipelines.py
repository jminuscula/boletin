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

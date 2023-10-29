from datetime import datetime
from unittest import mock

import pytest

from boedb.pipelines.step import StepPipeline


@pytest.mark.asyncio
async def test_pipeline_runs_etl():
    item = mock.Mock()
    extractor = mock.AsyncMock(return_value=item)

    transformed = mock.Mock()
    transformer = mock.AsyncMock(return_value=transformed)

    loaded = mock.Mock()
    loader = mock.AsyncMock(return_value=loaded)

    pipeline = StepPipeline(extractor, transformer, loader)
    processed = await pipeline.run()

    extractor.assert_called()
    transformer.assert_called_with(item)
    loader.assert_called_with(transformed)
    assert processed is loaded

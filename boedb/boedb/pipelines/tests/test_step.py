import asyncio
from unittest import mock

import pytest

from boedb.pipelines.step import BatchProcessorMixin, StepPipeline


@pytest.mark.asyncio
async def test_batch_processor_processes_all_items():
    items = list(range(10))
    processor = BatchProcessorMixin(2)
    processor.gather = mock.AsyncMock(return_value=items)
    processor.process = mock.AsyncMock(side_effect=lambda i: i)
    processed = await processor.process_in_batch()
    assert items == processed


@pytest.mark.asyncio
async def test_batch_processor_processes_smaller_batch():
    items = list(range(10))
    processor = BatchProcessorMixin(100)
    processor.gather = mock.AsyncMock(return_value=items)
    processor.process = mock.AsyncMock(side_effect=lambda i: i)
    processed = await processor.process_in_batch()
    assert items == processed


@pytest.mark.asyncio
async def test_batch_processor_runs_concurrently():
    with mock.patch("asyncio.TaskGroup", wraps=asyncio.TaskGroup) as tg_mock:
        items = list(range(10))
        processor = BatchProcessorMixin(2)
        processor.gather = mock.AsyncMock(return_value=items)
        processor.process = mock.AsyncMock(side_effect=lambda i: i)
        await processor.process_in_batch()
    assert len(tg_mock.call_args_list) == 5


@pytest.mark.asyncio
async def test_batch_processor_raises_if_gather_not_implemented():
    processor = BatchProcessorMixin(10)
    with pytest.raises(NotImplementedError):
        await processor.process_in_batch()


@pytest.mark.asyncio
async def test_batch_processor_raises_if_process_not_implemented():
    processor = BatchProcessorMixin(10)
    processor.gather = mock.AsyncMock(return_value=[1])
    with pytest.raises(ExceptionGroup):
        await processor.process_in_batch()


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

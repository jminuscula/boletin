import asyncio
from unittest import mock

import pytest

from boedb.processors.batch import BatchProcessor


@pytest.mark.asyncio
async def test_batch_processor_processes_all_items():
    items = list(range(10))
    processor = BatchProcessor(2)
    processor.gather = mock.AsyncMock(return_value=items)
    processor.process = mock.AsyncMock(side_effect=lambda i: i)
    processed = await processor.process_in_batch()
    assert items == processed


@pytest.mark.asyncio
async def test_batch_processor_processes_smaller_batch():
    items = list(range(10))
    processor = BatchProcessor(100)
    processor.gather = mock.AsyncMock(return_value=items)
    processor.process = mock.AsyncMock(side_effect=lambda i: i)
    processed = await processor.process_in_batch()
    assert items == processed


@pytest.mark.asyncio
async def test_batch_processor_runs_concurrently():
    with mock.patch("asyncio.TaskGroup", wraps=asyncio.TaskGroup) as tg_mock:
        items = list(range(10))
        processor = BatchProcessor(2)
        processor.gather = mock.AsyncMock(return_value=items)
        processor.process = mock.AsyncMock(side_effect=lambda i: i)
        await processor.process_in_batch()
    assert len(tg_mock.call_args_list) == 5


@pytest.mark.asyncio
async def test_batch_processor_raises_if_gather_not_implemented():
    processor = BatchProcessor(10)
    with pytest.raises(NotImplementedError):
        await processor.process_in_batch()


@pytest.mark.asyncio
async def test_batch_processor_raises_if_process_not_implemented():
    processor = BatchProcessor(10)
    processor.gather = mock.AsyncMock(return_value=[1])
    with pytest.raises(ExceptionGroup):
        await processor.process_in_batch()

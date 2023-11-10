import asyncio
from datetime import datetime
from unittest import mock

import pytest

from boedb.pipelines.stream import AsyncShutdownQueue, StreamPipeline, StreamPipelineBaseExecutor


@pytest.mark.asyncio
async def test_async_queue_can_aiter():
    queue = AsyncShutdownQueue()
    await queue.put(1)
    await queue.put(2)

    qiter = aiter(queue)
    assert await anext(qiter) == 1
    assert await anext(qiter) == 2


@pytest.mark.asyncio
async def test_async_queue_stops_iter_after_shutdown():
    queue = AsyncShutdownQueue()
    await queue.put(1)
    await queue.shutdown()

    assert await anext(queue) == 1
    with pytest.raises(StopAsyncIteration):
        await anext(queue)


@pytest.mark.asyncio
async def test_stream_pipeline_consumes_results():
    queue = AsyncShutdownQueue()
    await queue.put(1)
    await queue.put(2)
    await queue.shutdown()

    queue_mock = mock.AsyncMock()
    with mock.patch("boedb.pipelines.stream.AsyncShutdownQueue", return_value=queue_mock):
        pipeline = StreamPipeline(None, None, None)
        await pipeline.collect_results(queue)

    queue_mock.put.assert_has_awaits([mock.call(1), mock.call(2)])
    queue_mock.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_stream_pipeline_runs_pipeline():
    extractor = mock.Mock(start=mock.AsyncMock(return_value=True))
    extracted_queue = mock.AsyncMock(spec=AsyncShutdownQueue)
    extractor.get_output_queue.return_value = extracted_queue

    transformer = mock.Mock(start=mock.AsyncMock(return_value=True))
    transformed_queue = mock.AsyncMock(spec=AsyncShutdownQueue)
    transformer.get_output_queue.return_value = transformed_queue

    loader = mock.Mock(start=mock.AsyncMock(return_value=True))
    loaded_queue = mock.AsyncMock(spec=AsyncShutdownQueue)
    loader.get_output_queue.return_value = loaded_queue

    items = [1]
    with mock.patch("boedb.pipelines.stream.StreamPipeline.collect_results") as collect_mock:
        pipeline = StreamPipeline(extractor, transformer, loader)
        results = await pipeline.run_pipeline(items)

    pipeline.extractor.start.assert_awaited_once_with(items, extracted_queue)
    pipeline.transformer.start.assert_awaited_once_with(extracted_queue, transformed_queue)
    pipeline.loader.start.assert_awaited_once_with(transformed_queue, loaded_queue)
    collect_mock.assert_called_once_with(loaded_queue)
    assert results == [True, True, True, collect_mock.return_value]


@pytest.mark.asyncio
async def test_stream_pipeline_runs_pipeline_returns_exceptions():
    extractor = mock.Mock(start=mock.AsyncMock(return_value=True))
    extracted_queue = mock.AsyncMock(spec=AsyncShutdownQueue)
    extractor.get_output_queue.return_value = extracted_queue

    error = Exception()
    transformer = mock.Mock(start=mock.AsyncMock(side_effect=error))
    transformed_queue = mock.AsyncMock(spec=AsyncShutdownQueue)
    transformer.get_output_queue.return_value = transformed_queue

    loader = mock.Mock(start=mock.AsyncMock(return_value=True))
    loaded_queue = mock.AsyncMock(spec=AsyncShutdownQueue)
    loader.get_output_queue.return_value = loaded_queue

    items = [1]
    with mock.patch("boedb.pipelines.stream.StreamPipeline.collect_results") as collect_mock:
        pipeline = StreamPipeline(extractor, transformer, loader)
        results = await pipeline.run_pipeline(items)

    pipeline.extractor.start.assert_awaited_once_with(items, extracted_queue)
    pipeline.transformer.start.assert_awaited_once_with(extracted_queue, transformed_queue)
    pipeline.loader.start.assert_awaited_once_with(transformed_queue, loaded_queue)
    collect_mock.assert_called_once_with(loaded_queue)
    assert results == [True, error, True, collect_mock.return_value]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_stream_pipeline_run_ok():
    pipeline = StreamPipeline(
        extractor=StreamPipelineBaseExecutor(1),
        transformer=StreamPipelineBaseExecutor(1),
        loader=StreamPipelineBaseExecutor(1),
    )

    results = []
    async for item in pipeline.run([1, 2, 3]):
        results.append(item)

    assert results == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_stream_pipeline_run_raises_with_exception():
    error = ValueError()

    class FailingPhase(StreamPipelineBaseExecutor):
        async def process(self, _):
            raise error

    pipeline = StreamPipeline(
        extractor=StreamPipelineBaseExecutor(1),
        transformer=FailingPhase(1),
        loader=StreamPipelineBaseExecutor(1),
    )

    with pytest.raises(ValueError):
        await pipeline.run_and_collect([1, 2, 3])


@pytest.mark.asyncio
@pytest.mark.integration
async def test_stream_pipeline_waits_for_tasks_on_shutdown():
    error = ValueError()

    class FailingPhase(StreamPipelineBaseExecutor):
        async def process(self, _):
            raise error

    pipeline = StreamPipeline(
        extractor=StreamPipelineBaseExecutor(1),
        transformer=FailingPhase(1),
        loader=StreamPipelineBaseExecutor(1),
    )

    with pytest.raises(ValueError):
        task = asyncio.create_task(asyncio.sleep(0.01))
        await pipeline.run_and_collect([1, 2, 3])

    assert task.done() is True


@pytest.mark.asyncio
@pytest.mark.integration
async def test_stream_pipeline_run_and_collect_ok():
    pipeline = StreamPipeline(
        extractor=StreamPipelineBaseExecutor(1),
        transformer=StreamPipelineBaseExecutor(1),
        loader=StreamPipelineBaseExecutor(1),
    )

    results = await pipeline.run_and_collect([1, 2, 3])
    assert results == [1, 2, 3]

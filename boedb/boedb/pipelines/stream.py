import asyncio
import random
from abc import abstractmethod


class BaseStreamExtractor:
    @abstractmethod
    async def start_producer(self, dest_queue):
        raise NotImplementedError


class BaseStreamTransformer:
    @abstractmethod
    async def start_consumer(self, source_queue):
        raise NotImplementedError

    @abstractmethod
    async def start_producer(self, dest_queue):
        raise NotImplementedError


class BaseStreamLoader:
    @abstractmethod
    async def start_consumer(self, source_queue):
        raise NotImplementedError

    @abstractmethod
    async def start_producer(self, dest_queue):
        raise NotImplementedError


class AsyncShutdownQueue(asyncio.Queue):
    QUEUE_END = object()

    def __aiter__(self):
        return self

    async def __anext__(self):
        entry_task = await self.get()
        if entry_task is self.QUEUE_END:
            self.task_done()
            raise StopAsyncIteration

        return entry_task

    async def shutdown(self):
        await self.put(self.QUEUE_END)


class StreamPipelineBaseExecutor:
    """
    Each phase takes the task produced by the phase before it, and awaits the result
    before it operates on it.
    """

    def __init__(self, concurrency):
        self.concurrency = concurrency
        self.output_queue = AsyncShutdownQueue(maxsize=concurrency)

    def get_output_queue(self):
        return self.output_queue

    def get_task_name(self, name):
        return f"Pipeline({self.__class__.__name__}) {name}"

    @abstractmethod
    async def process(self, item):
        raise NotImplementedError

    async def get_jobs_from_iterable(self, iterable, work_queue):
        for item in iterable:
            await work_queue.put(item)

        await work_queue.shutdown()

    async def get_jobs_from_queue(self, entry_queue, work_queue):
        async for item in entry_queue:
            job = item
            if isinstance(item, asyncio.Task):
                await item
                job = item.result()
            await work_queue.put(job)
            entry_queue.task_done()

        await work_queue.shutdown()

    async def process_jobs(self, work_queue, results_queue):
        async for item in work_queue:
            job = item
            if isinstance(item, asyncio.Task):
                await item
                job = item.result()
            task = asyncio.create_task(self.process(job), name=self.get_task_name(f"process {job}"))
            await results_queue.put(task)
            work_queue.task_done()

        await results_queue.shutdown()

    async def start(self, entry_queue_or_iterable, results_queue):
        # Concurrency is controlled by getting jobs from the entry queue
        # and adding them to this limited queue. It effectively acts as a
        # buffer to limit the number of process tasks launched.
        work_queue = AsyncShutdownQueue(maxsize=self.concurrency)

        if isinstance(entry_queue_or_iterable, list):
            jobs_task = asyncio.create_task(
                self.get_jobs_from_iterable(entry_queue_or_iterable, work_queue),
                name=self.get_task_name("get jobs from iter"),
            )
        else:
            jobs_task = asyncio.create_task(
                self.get_jobs_from_queue(entry_queue_or_iterable, work_queue),
                name=self.get_task_name("get jobs from queue"),
            )

        process_task = asyncio.create_task(
            self.process_jobs(work_queue, results_queue), name=self.get_task_name("process jobs")
        )

        return await asyncio.gather(jobs_task, process_task)


class StreamPipeline:
    """
    Orchestrates the ETL pipeline by streaming items through each phase as they
    become available. Each phase provides a limited output queue that is used to
    control concurrency, as a step won't continue producing until the output queue
    can accommodate a new item.

    The extractor, transformer and loader instances are `StreamPipelineBaseExecutor`.

    :param extractor: extract items from entry point into output queue
    :param transformer: transform items from entry queue into output queue
    :param loader: load items from entry queue onto results queue
    """

    def __init__(self, extractor, transformer=None, loader=None):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

        # output needs not to be limited, so we can exhaust the pipeline
        self.results_queue = AsyncShutdownQueue()

    async def collect_results(self, queue):
        async for item in queue:
            await self.results_queue.put(item)
        await queue.shutdown()

    async def run_pipeline(self, items):
        extracted_queue = self.extractor.get_output_queue()
        transformed_queue = self.transformer.get_output_queue()
        loaded_queue = self.loader.get_output_queue()

        extract_task = self.extractor.start(items, extracted_queue)
        transform_task = self.transformer.start(extracted_queue, transformed_queue)
        load_task = self.loader.start(transformed_queue, loaded_queue)

        collect_task = asyncio.create_task(self.collect_results(loaded_queue))
        return await asyncio.gather(extract_task, transform_task, load_task, collect_task)

    async def run(self, items):
        await self.run_pipeline(items)
        await self.results_queue.shutdown()

        async for item in self.results_queue:
            await item
            yield item.result()
            self.results_queue.task_done()

    async def run_and_collect(self, items):
        results = []
        async for item in self.run(items):
            results.append(item)
        return results

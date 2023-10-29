import asyncio
from abc import abstractmethod
from itertools import islice

from boedb.config import get_logger


class BaseStepExtractor:
    @abstractmethod
    async def __call__(self):
        raise NotImplementedError


class BaseStepTransformer:
    @abstractmethod
    async def __call__(self, items):
        raise NotImplementedError


class BaseStepLoader:
    @abstractmethod
    async def __call__(self, items):
        raise NotImplementedError


def batched(iterable, n):  # pragma: no cover
    """Batch data into tuples of length n. The last batch may be shorter.
    https://docs.python.org/3/library/itertools.html
    """
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


class BatchProcessorMixin:
    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.logger = get_logger("boedb.batchprocessor")

    async def gather(self):
        raise NotImplementedError

    async def process(self, item):
        raise NotImplementedError

    async def process_in_batch(self, items=None):
        tasks = []
        if items is None:
            items = await self.gather()

        processed = 0
        for batch in batched(items, self.batch_size):
            async with asyncio.TaskGroup() as tg:
                for item in batch:
                    task = tg.create_task(self.process(item))
                    tasks.append(task)
            processed += len(batch)
            self.logger.debug(f"{processed}/{len(items)} items processed")
        return [task.result() for task in tasks]


class StepPipeline:
    """
    Orchestrates the ETL pipeline.
    The extractor, transformer and loader callables are asynchronous functions responsible
    for implementing their own concurrency mechanisms.

    :param extractor: async callable to produce items
    :param transformer: async callable to transform extracted items
    :param loader: async callable to load transformed items
    """

    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    async def run(self):
        items = await self.extractor()
        if self.transformer:
            items = await self.transformer(items)
        if self.loader:
            items = await self.loader(items)
        return items

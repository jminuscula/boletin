import asyncio
from abc import abstractmethod

from boedb.config import get_logger
from boedb.processors.batch import batched


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
    Orchestrates the ETL pipeline in steps, so each phase takes the previous result as an
    argument, and is only started once the previous phase ends.

    The extractor, transformer and loader are asynchronous callables responsible for
    implementing their own concurrency mechanisms.

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
        if items is not None and self.transformer:
            items = await self.transformer(items)
        if items is not None and self.loader:
            items = await self.loader(items)
        return items

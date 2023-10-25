import asyncio
from itertools import islice


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


class BatchProcessor:
    def __init__(self, batch_size):
        self.batch_size = batch_size

    async def gather(self):
        raise NotImplementedError

    async def process(self, item):
        raise NotImplementedError

    async def process_in_batch(self, items=None):
        tasks = []
        if items is None:
            items = await self.gather()
        for batch in batched(items, self.batch_size):
            async with asyncio.TaskGroup() as tg:
                for item in batch:
                    task = tg.create_task(self.process(item))
                    tasks.append(task)
        return [task.result() for task in tasks]

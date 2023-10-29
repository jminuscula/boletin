class StreamPipeline:
    """
    test
    """

    def __init__(self, extractor, transformer=None, loader=None):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    async def run(self):
        transform_queue = asyncio.Queue()
        async for item in self.extractor():
            await transform_queue.put(item)
            async for transformed in self.transformer(transform_queue):
                print("TRANSFORMED", transformed)
                if self.loader:
                    item = await self.loader(transformed)

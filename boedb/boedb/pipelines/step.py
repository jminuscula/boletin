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

import itertools

import xmltodict

from boedb.diario_boe.models import Article, DaySummary
from boedb.processors.batch import BatchProcessor

BASE_URL = "https://www.boe.es"


async def extract_boe_document(cls, doc_id, session):
    # breakpoint()
    url = f"{BASE_URL}/diario_boe/xml.php?id={doc_id}"
    async with session.get(url) as resp:
        xml = await resp.text()
    data = xmltodict.parse(xml)
    return cls.from_dict(data)


class DiarioBoeSummaryExtractor:
    def __init__(self, date, http_session):
        self.date = date
        self.http_session = http_session

    async def __call__(self):
        summary_id = f"BOE-S-{self.date.strftime('%Y%m%d')}"
        return await extract_boe_document(DaySummary, summary_id, self.http_session)


class DiarioBoeArticlesExtractor(BatchProcessor):
    def __init__(self, summary, http_session, batch_size=10):
        self.summary = summary
        self.http_session = http_session
        super().__init__(batch_size)

    async def gather(self):
        return self.summary.items

    async def process(self, item):
        return await extract_boe_document(Article, item.entry_id, self.http_session)

    async def __call__(self):
        articles = await self.process_in_batch()
        return list(itertools.chain.from_iterable(a.split() for a in articles))

import itertools
from xml.etree import ElementTree

from boedb.config import get_logger
from boedb.diario_boe.models import Article, DaySummary
from boedb.processors.batch import BatchProcessor

BASE_URL = "https://www.boe.es"


async def extract_boe_document(cls, doc_id, session):
    url = f"{BASE_URL}/diario_boe/xml.php"
    async with session.get(url, params={"id": doc_id}) as resp:
        xml = await resp.text()
    root = ElementTree.fromstring(xml)
    return cls.from_xml(root)


class DiarioBoeSummaryExtractor:
    def __init__(self, date, http_session):
        self.date = date
        self.http_session = http_session
        self.logger = get_logger("boedb.diario_boe.summary_extractor")

    async def __call__(self):
        summary_id = f"BOE-S-{self.date.strftime('%Y%m%d')}"
        doc = await extract_boe_document(DaySummary, summary_id, self.http_session)

        self.logger.info(f"Extracted summary {summary_id}")
        return doc


class DiarioBoeArticlesExtractor(BatchProcessor):
    def __init__(self, summary, http_session, batch_size=10):
        super().__init__(batch_size)

        self.summary = summary
        self.http_session = http_session
        self.logger = get_logger("boedb.diario_boe.articles_extractor")

    async def gather(self):
        return self.summary.items

    async def process(self, item):
        doc = await extract_boe_document(Article, item.entry_id, self.http_session)

        self.logger.debug(f"Extracted article {item.entry_id}")
        return doc

    async def __call__(self):
        articles = await self.process_in_batch()
        fragments = list(itertools.chain.from_iterable(a.split() for a in articles))

        self.logger.info(f"Extracted {len(fragments)} fragments for {len(articles)} articles")
        return fragments

import asyncio
import itertools
from xml.etree import ElementTree

from boedb.config import get_logger
from boedb.diario_boe.models import Article, DaySummary
from boedb.pipelines.step import BaseStepTransformer, BatchProcessorMixin
from boedb.processors.batch import batched

BASE_URL = "https://www.boe.es"


async def extract_boe_xml(doc_id, session):
    url = f"{BASE_URL}/diario_boe/xml.php"
    async with session.get(url, params={"id": doc_id}) as resp:
        resp.raise_for_status()
        xml = await resp.text()
    return ElementTree.fromstring(xml)


async def extract_boe_summary(summary_id, session):
    xml = await extract_boe_xml(summary_id, session)
    return DaySummary.from_xml(xml)


async def extract_boe_article(article_id, summary_id, session):
    xml = await extract_boe_xml(article_id, session)
    return Article.from_xml(xml, summary_id)


class DiarioBoeSummaryExtractor(BaseStepTransformer):
    def __init__(self, date, http_session):
        self.date = date
        self.http_session = http_session
        self.logger = get_logger("boedb.diario_boe.summary_extractor")

    async def __call__(self):
        summary_id = f"BOE-S-{self.date.strftime('%Y%m%d')}"
        doc = await extract_boe_summary(summary_id, self.http_session)

        self.logger.info(f"Extracted summary {summary_id}")
        return doc


class DiarioBoeArticlesExtractor(BatchProcessorMixin, BaseStepTransformer):
    def __init__(self, summary, http_session, batch_size=10):
        super().__init__(batch_size)

        self.summary = summary
        self.http_session = http_session
        self.logger = get_logger("boedb.diario_boe.articles_extractor")

    async def gather(self):
        return self.summary.items

    async def process(self, item):
        doc = await extract_boe_article(item.entry_id, self.summary.summary_id, self.http_session)

        self.logger.debug(f"Extracted article {item.entry_id}")
        return doc

    async def __call__(self):
        articles = await self.process_in_batch()
        fragments = list(itertools.chain.from_iterable(a.split() for a in articles))

        self.logger.info(f"Extracted {len(fragments)} fragments from {len(articles)} articles")
        return fragments

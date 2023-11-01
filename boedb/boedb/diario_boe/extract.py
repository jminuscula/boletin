from xml.etree import ElementTree

from boedb.config import get_logger
from boedb.diario_boe.models import Article, DaySummary
from boedb.pipelines.step import BaseStepExtractor
from boedb.pipelines.stream import StreamPipelineBaseExecutor

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


class SummaryExtractor(BaseStepExtractor):
    def __init__(self, date, http_session):
        self.date = date
        self.http_session = http_session
        self.logger = get_logger("boedb.diario_boe.summary_extractor")

    async def __call__(self):
        summary_id = f"BOE-S-{self.date.strftime('%Y%m%d')}"
        doc = await extract_boe_summary(summary_id, self.http_session)

        self.logger.info(f"Extracted summary {summary_id}")
        return doc


class ArticlesExtractor(StreamPipelineBaseExecutor):
    def __init__(self, concurrency, http_session):
        self.logger = get_logger("boedb.diario_boe.article_extractor")
        self.http_session = http_session
        super().__init__(concurrency)

    async def process(self, item):
        doc = await extract_boe_article(item.entry_id, item.summary_id, self.http_session)

        self.logger.debug(f"Extracted {doc}")
        return doc.split()

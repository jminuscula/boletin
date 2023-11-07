from xml.etree import ElementTree

from boedb.client import HttpClient
from boedb.config import get_logger
from boedb.diario_boe.models import Article, DaySummary
from boedb.pipelines.step import BaseStepExtractor
from boedb.pipelines.stream import StreamPipelineBaseExecutor

BASE_URL = "https://www.boe.es"


async def extract_boe_xml(doc_id, client):
    url = f"{BASE_URL}/diario_boe/xml.php?id={doc_id}"
    xml = await client.get(url, parse_response=False)
    return ElementTree.fromstring(xml)


async def extract_boe_summary(summary_id, client):
    xml = await extract_boe_xml(summary_id, client)
    return DaySummary.from_xml(xml)


async def extract_boe_article(article_id, summary_id, client):
    xml = await extract_boe_xml(article_id, client)
    return Article.from_xml(xml, summary_id)


class SummaryExtractor(BaseStepExtractor):
    def __init__(self, date, http_session, should_skip=None):
        self.date = date
        self.should_skip = should_skip
        self.client = HttpClient(http_session)
        self.logger = get_logger("boedb.diario_boe.summary_extractor")

    async def __call__(self):
        summary_id = f"BOE-S-{self.date.strftime('%Y%m%d')}"
        doc = await extract_boe_summary(summary_id, self.client)

        if self.should_skip is not None and self.should_skip(doc):
            self.logger.info(f"Skipping {doc.summary_id}")
            return

        self.logger.info(f"Extracted summary {summary_id}")
        return doc


class ArticlesExtractor(StreamPipelineBaseExecutor):
    def __init__(self, concurrency, http_session, should_skip=None):
        self.logger = get_logger("boedb.diario_boe.article_extractor")
        self.client = HttpClient(http_session)
        self.should_skip = should_skip
        super().__init__(concurrency)

    async def process(self, item):
        if self.should_skip is not None and self.should_skip(item):
            self.logger.debug(f"Skipping {item}")
            return

        doc = await extract_boe_article(item.entry_id, item.summary_id, self.client)

        fragments = doc.split()
        self.logger.debug(f"Extracted {doc}")
        return [doc, *fragments]

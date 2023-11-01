import time

from boedb.config import get_logger
from boedb.pipelines.stream import StreamPipelineBaseExecutor
from boedb.processors.html import HTMLFilter
from boedb.processors.llm import OpenAiClient


class ArticlesTransformer(StreamPipelineBaseExecutor):
    def __init__(self, concurrency, http_session):
        self.logger = get_logger("boedb.diario_boe.article_transformer")
        super().__init__(concurrency)

        self.llm_client = OpenAiClient(http_session)

    @staticmethod
    def get_title_summary_prompt(title):
        return [
            {
                "role": "system",
                "content": "You are a legal clerk helping make spanish documents easier to understand.",
            },
            {
                "role": "system",
                "content": (
                    "You respond in a concise, clear and natural way. "
                    "You avoid redundant information and excessive detail."
                ),
            },
            {
                "role": "system",
                "content": "You avoid including signatures, generic openings and endings.",
            },
            {
                "role": "user",
                "content": f'Reescribe este título para hacerlo más corto, conciso y simple: "{title}".',
            },
        ]

    @staticmethod
    def get_content_summary_prompt(content):
        return [
            {
                "role": "system",
                "content": "You are a legal clerk helping make spanish documents easier to understand.",
            },
            {
                "role": "system",
                "content": (
                    "You respond in a concise, clear and natural way. "
                    "You avoid redundant information and excessive detail."
                ),
            },
            {
                "role": "system",
                "content": "You avoid including signatures, generic openings and endings.",
            },
            {
                "role": "user",
                "content": f'Reescribe este texto para hacerlo más corto, conciso y simple: "{content}".',
            },
        ]

    async def process(self, item):
        start_time = time.time()
        self.logger.debug(f"Transforming {item}")

        if item.title:
            # LLM will truncate the output on max_tokens, which should be ok since
            # we are expecting the summary to be 1/2 or 1/3 of the original length
            max_tokens = len(item.title) // 2
            title_summary_prompt = self.get_title_summary_prompt(item.title)
            item.title_summary = await self.llm_client.complete(title_summary_prompt, max_tokens=max_tokens)
            item.title_embedding = await self.llm_client.get_embeddings(item.title)

        # We want to avoid using LLM tokens for the html tags, and there are
        # certain elements (eg. tables) that won't provide much meaningful content
        clean_content = HTMLFilter.clean_html(item.content)
        if clean_content:
            max_tokens = len(clean_content) // 3
            summary_prompt = self.get_content_summary_prompt(clean_content)
            item.summary = await self.llm_client.complete(summary_prompt, max_tokens=max_tokens)
            item.embedding = await self.llm_client.get_embeddings(clean_content)

        end_time = time.time() - start_time
        self.logger.debug(f"Transformed {item} ({end_time:.2f}s)")
        return item

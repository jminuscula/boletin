from abc import abstractmethod

from boedb.db import get_db_client
from boedb.processors.llm import OpenAiClient


class QueryManager:
    def __init__(self, http_session):
        self.llm_client = OpenAiClient(http_session)
        self.db_client = get_db_client()

    @abstractmethod
    def get_text_query(query):
        raise NotImplementedError

    @abstractmethod
    def get_vector_query(self, embedding):
        raise NotImplementedError

    @abstractmethod
    async def rank_query(self, text_results, vector_results):
        raise NotImplementedError

    async def query(self, query):
        embedding = await self.llm_client.get_embeddings(query)
        text_q = self.get_text_query(query)
        text_results = self.get_text_results(text_q)

        vector_q = self.get_vector_query(embedding)
        vector_results = self.get_vector_results(vector_q)

        rank = await self.rank_query(text_results, vector_results)
        extract = await self.extract(query, rank)

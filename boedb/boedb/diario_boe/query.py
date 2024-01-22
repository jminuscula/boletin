import asyncio
import sys
import textwrap
from collections import defaultdict
from itertools import zip_longest

from boedb.client import get_http_client_session
from boedb.db import get_db_client
from boedb.processors.html import HTMLFilter
from boedb.processors.llm import OpenAiClient


class DiarioBoeQueryManager:
    def __init__(self, http_session):
        self.llm_client = OpenAiClient(http_session)
        self.db_client = get_db_client()

    def search_articles(self, query, embedding, max_results=10):
        text_results = self.db_client.execute(
            r"""
                select
                    article_id,
                    ts_rank(title_search, plainto_tsquery('spanish', %s)) rank
                from
                    es_diario_boe_article
                where
                    title_search @@ plainto_tsquery('spanish', %s)
                order by
                    rank desc
                limit
                    %s;
            """,
            (query, query, max_results),
        )

        vector_results = self.db_client.execute(
            r"""
                select
                    article_id
                from
                    es_diario_boe_article
                order by
                    title_embedding <-> %s::vector
                limit
                    %s;
            """,
            (embedding, max_results),
        )

        return self.combine_results(text_results, vector_results)

    def search_fragments(self, query, embedding, max_results=10):
        text_results = self.db_client.execute(
            r"""
                select
                    article_id, sequence, content, summary,
                    ts_rank(content_search, plainto_tsquery('spanish', %s)) rank
                from
                    es_diario_boe_article_fragment
                where
                    content_search @@ plainto_tsquery('spanish', %s)
                limit
                    %s;
            """,
            (query, query, max_results),
        )

        vector_results = self.db_client.execute(
            r"""
                select
                    article_id, sequence, content, summary
                from
                    es_diario_boe_article_fragment
                order by
                    embedding <-> %s::vector
                limit
                    %s;
            """,
            (embedding, max_results),
        )

        return self.combine_results(text_results, vector_results)

    def combine_results(self, text_results, vector_results, max_results=10):
        all_results = {res["article_id"]: res for res in text_results}
        all_results.update({res["article_id"]: res for res in vector_results})

        article_scores = defaultdict(lambda: 0)
        for idx, (t_res, v_res) in enumerate(zip_longest(text_results, vector_results)):
            if t_res is not None:
                aid = t_res["article_id"]
                article_scores[aid] += idx if aid in article_scores else -1
            if v_res is not None:
                aid = v_res["article_id"]
                article_scores[aid] += idx if aid in article_scores else -1

        rank = sorted(article_scores, key=lambda d: article_scores[d])
        return tuple(all_results[res] for res in rank[:max_results])

    def rank(self, articles, fragments, max_results=10):
        article_ids = {res["article_id"] for res in articles}
        fragment_id_map = {}
        fragment_scores = {}
        for idx, fragment in enumerate(fragments, 1):
            score = idx
            fragment_id = fragment["article_id"]
            sequence = fragment["sequence"]
            if fragment_id in article_ids:
                score = idx / 2

            fragment_id_map[(fragment_id, sequence)] = fragment
            fragment_scores[(fragment_id, sequence)] = score

        scores = sorted(fragment_scores, key=lambda fr: fragment_scores[fr])
        ranked = [fragment_id_map[fr_id, seq] for fr_id, seq in scores]
        return ranked[:max_results]

    async def extract(self, query, ranking):
        fragments = "\n\n".join(fr["summary"] or fr["content"] for fr in ranking)
        source_documents = HTMLFilter.clean_html(fragments)
        prompt = [
            {
                "role": "system",
                "content": textwrap.dedent(
                    """
                    Eres un abogado español proporcionando asistencia legal.
                    """
                ),
            },
            {
                "role": "user",
                "content": textwrap.dedent(
                    f"""
                    Basándote en los documentos encontrados, escribe una breve respuesta o explicación
                    a la pregunta o asunto proporcionados. Descarta los documentos no relevantes.
                    Si existe una respuesta simple, escríbela directamente. Si no, escribe una breve
                    explicación.

                    Asunto: "{query}"
                    Documentos:
                    {source_documents}
                    """
                ),
            },
        ]

        return await self.llm_client.complete(prompt)

    async def query(self, query):
        embedding = await self.llm_client.get_embeddings(query)
        relevant_articles = self.search_articles(query, embedding)
        relevant_fragments = self.search_fragments(query, embedding)

        rank = self.rank(relevant_articles, relevant_fragments)
        return await self.extract(query, rank)


if __name__ == "__main__":

    async def prompt():
        async with get_http_client_session() as http_session:
            qm = DiarioBoeQueryManager(http_session)
            while True:
                input_query = input("query> ")
                result = await qm.query(input_query)
                print(result)
                print()

    try:
        asyncio.run(prompt())
    except (EOFError, KeyboardInterrupt):
        sys.exit(0)

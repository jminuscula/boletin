import functools
import json
import re
from datetime import datetime

from boedb.config import DiarioBoeConfig
from boedb.processors.transformers import extract_keys_with_metadata
from boedb.processors.xml import (
    find_node_with_ancestors,
    node_children_to_dict,
    node_text_content,
)


class DocumentError(Exception):
    pass


def check_error(fn):
    @functools.wraps(fn)
    def from_xml(cls, root):
        if root.tag == "error":
            raise DocumentError(root.find(".//descripcion").text or "unknown error")
        return fn(cls, root)

    return from_xml


class DaySummary:
    def __init__(self, summary_id, metadata, items=None):
        self.summary_id = summary_id
        self.publication_date = datetime.strptime(metadata["fecha"], "%d/%m/%Y")
        self.metadata = metadata
        self.items = items or []

    @classmethod
    @check_error
    def from_xml(cls, root):
        summary_id = root.find("diario/sumario_nbo").attrib.get("id")
        summary_metadata = node_children_to_dict(root.find("meta"))

        items = []
        for item, ancestors in find_node_with_ancestors(root, "item"):
            meta = {a.tag: a.attrib for a in ancestors}
            meta |= item.attrib
            title = item.find(".//titulo").text
            item = DaySummaryEntry(summary_id, item.attrib["id"], meta, title)
            items.append(item)
        return cls(summary_id, summary_metadata, items)

    def as_dict(self):
        return {
            "id": self.summary_id,
            "publication_date": self.publication_date,
            "metadata": json.dumps(self.metadata),
        }

    def __repr__(self):
        return f"DaySummary({self.summary_id})"


class DaySummaryEntry:
    def __init__(self, summary_id, entry_id, metadata=None, title=None):
        self.summary_id = summary_id
        self.entry_id = entry_id
        self.metadata = metadata or {}
        self.title = title

    def __repr__(self):
        return f"DaySummaryEntry({self.summary_id}/{self.entry_id})"


class Article:
    def __init__(self, article_id, metadata, content, sequence=None, total=None):
        self.article_id = article_id
        self.publication_date = datetime.strptime(metadata["fecha_publicacion"], "%Y%m%d")
        self.metadata = metadata

        self.title = metadata.get("titulo")
        self.title_summary = None
        self.title_embedding = None

        self.content = content
        self.summary = None
        self.embedding = None
        self.sequence = sequence
        self.total = total

    @classmethod
    @check_error
    def from_xml(cls, root):
        article_id = root.find(".//identificador").text
        metadata = {
            **node_children_to_dict(root.find("./metadatos")),
            **node_children_to_dict(root.find("./analisis")),
        }
        content = node_text_content(root.find("./texto"))
        return cls(article_id, metadata, content)

    @staticmethod
    def _split_text_smart(text, max_length):
        re_breaks = (
            r"<(.*?)>\s*(ANEXO|ANEJO).*?</(.*?)>",
            r"<(.*?)>\s*Artículo.*?</(.*?)>",
            r"<(.*?)>\s*Título.*?</(.*?)>",
            r"<(.*?)>\s*(Reunidos|Manifiestan|Exponen|Cláusulas).*?</(.*?)>",
            r"<(.*?)>\s*Fundamentos de Derecho.*?</(.*?)>",
            r"<table(.*?)>(.*?)</table>",
            r"<(.*?)>\s*Fundamentos jurídicos.*?</(.*?)>",
            (
                r"<(.*?)>\s*"
                r"(Primer[oa]|Segund[oa]|Tercer[oa]|Cuart[oa]|Quint[oa]|Sext[oa]|Séptim[oa]"
                r"|Octav[oa]|Noven[oa]|Décim[oa]|Undécim[oa]|Duodécim[oa]"
                r"|Decimotercer[oa]|Decimocuart[oa]|Decimoquint[oa]|Decimosext[oa]"
                r"|Decimoséptim[oa]|Decimoctav[oa]|Decimonoven[oa]|Vigésim[oa]"
                r"|Único)"
                r".*?</(.*?)>"
            ),
        )

        # try to split on text structure
        for break_exp in re_breaks:
            for match in re.finditer(break_exp, text, re.IGNORECASE):
                # only split if break point doesn't yield highly unequal chunks
                if 0.2 < (match.start() / len(text)) < 0.8:
                    return text[: match.start()], text[match.start() :]

        # try to split naively on middle paragraph to avoid breaking html
        closing_tags = list(re.finditer("</p>", text[:max_length]))
        if closing_tags:
            match = closing_tags[len(closing_tags) // 2]
            return text[: match.end()], text[match.end() :]

        # split desperately
        if len(text) < (max_length * 2):
            middle = len(text) // 2
            return text[:middle], text[middle:]
        return text[:max_length], text[max_length:]

    @staticmethod
    def _split_text(text, max_length):
        fragments = [text]
        while any(len(fr) > max_length for fr in fragments):
            idx_max = max(range(len(fragments)), key=lambda i: len(fragments[i]))
            fragment = fragments.pop(idx_max)
            sub_fragments = Article._split_text_smart(fragment, max_length)
            fragments = fragments[:idx_max] + list(sub_fragments) + fragments[idx_max:]
        return fragments

    def split(self, max_length=DiarioBoeConfig.ARTICLE_FRAGMENT_MAX_LENGTH):
        fragments = Article._split_text(self.content, max_length)
        total = len(fragments)
        return [
            self.__class__(self.article_id, self.metadata, fragment, seq, total)
            for seq, fragment in enumerate(fragments, 1)
        ]

    def as_article_dict(self):
        return {
            "article_id": self.article_id,
            "pubdate": self.publication_date,
            "metadata": json.dumps(self.metadata),
            "title": self.title,
            "title_summary": self.title_summary,
            "title_embedding": self.title_embedding,
        }

    def as_fragment_dict(self):
        return {
            "article_id": self.article_id,
            "sequence": self.sequence,
            "content": self.content,
            "summary": self.summary,
            "embedding": self.embedding,
        }

    def __repr__(self):
        seq = f", {self.sequence}/{self.total}" if self.sequence else ""
        return f"Article({self.article_id}{seq})"

import functools
from datetime import datetime

from boedb.processors.transformers import extract_keys_with_metadata


class DocumentError(Exception):
    pass


def check_error(fn):
    @functools.wraps(fn)
    def from_dict(cls, data):
        if isinstance(data, dict) and "error" in data:
            raise DocumentError(data["error"].get("descripcion", "unknown error"))
        return fn(cls, data)

    return from_dict


class DaySummary:
    def __init__(self, summary_id, metadata, items=None):
        self.summary_id = summary_id
        self.publication_date = datetime.strptime(metadata["fecha"], "%d/%m/%Y")
        self.metadata = metadata
        self.items = items or []

    @classmethod
    @check_error
    def from_dict(cls, data):
        sumario = data["sumario"]
        summary_id = sumario["diario"]["sumario_nbo"]["@id"]
        summary_metadata = sumario["meta"]

        items = []
        secciones = sumario["diario"]["seccion"]
        if type(secciones) is not list:
            secciones = [secciones]

        for item, meta in extract_keys_with_metadata(sumario["diario"], "item", "diario"):
            item = DaySummaryEntry(summary_id, item["@id"], meta, item["titulo"])
            items.append(item)

        return cls(summary_id, summary_metadata, items)

    def as_dict(self):
        return {
            "id": self.summary_id,
            "publication_date": self.publication_date,
            "metadata": self.metadata,
        }

    def __str__(self):
        return f"DaySummary({self.summary_id})"


class DaySummaryEntry:
    def __init__(self, summary_id, entry_id, metadata=None, title=None):
        self.summary_id = summary_id
        self.entry_id = entry_id
        self.metadata = metadata or {}
        self.title = title

    def __str__(self):
        return f"DaySummaryEntry({self.summary_id}/{self.entry_id})"


class Article:
    def __init__(self, article_id, metadata, content, sequence=None, total=None):
        self.article_id = article_id
        self.publication_date = datetime.strptime(metadata["fecha_publicacion"], "%Y%m%d")
        self.metadata = metadata

        self.title = metadata.get("titulo")
        self.title_summary = None
        self.title_embeddings = None

        self.content = content
        self.summary = None
        self.embeddings = None
        self.sequence = sequence
        self.total = total

    @classmethod
    @check_error
    def from_dict(cls, data):
        article_id = data["documento"]["metadatos"]["identificador"]
        metadata = {
            **data["documento"]["metadatos"],
            **(data["documento"].get("analisis") or {}),
        }
        content = data["documento"].get("texto", "")
        return cls(article_id, metadata, content)

    def split(self, max_length=2048):
        fragments = [self.fragments]
        total = len(fragments)
        return [
            self.__class__(self.article_id, self.metadata, fragment, seq, total)
            for seq, fragment in enumerate(fragments, 1)
        ]

    def __str__(self):
        seq = f", {self.sequence}/{self.total}" if self.sequence else ""
        return f"Article({self.article_id}{seq})"

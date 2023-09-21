import os.path
from datetime import datetime
from unittest import mock

import pytest
import textwrap
from xml.etree import ElementTree

from boedb.diario_boe.models import (
    Article,
    DaySummary,
    DaySummaryEntry,
    DocumentError,
    check_error,
)


@pytest.fixture
def summary_xml():
    return """
    <sumario>
        <diario>
            <sumario_nbo id="summary_id"></sumario_nbo>
            <item id="article_id">
                <titulo>test title</titulo>
            </item>
        </diario>
        <meta>
            <fecha>14/09/2023</fecha>
        </meta>
    </sumario>
    """


def xml_from_fixture(path):
    path = os.path.join(os.path.dirname(__file__), path)
    with open(path) as f:
        return ElementTree.fromstring(f.read())


@pytest.fixture
def article_data():
    return xml_from_fixture("fixtures/BOE-A-2023-18664.xml")


@pytest.fixture
def article_mtext_data():
    return xml_from_fixture("fixtures/BOE-A-2023-19658.xml")


def test_check_error_returns_document_error():
    @check_error
    def get_test_document(cls, root):
        return mock.Mock()

    root = ElementTree.fromstring(
        "<error><descripcion>test error</descripcion></error>"
    )
    with pytest.raises(DocumentError):
        get_test_document(None, root)


def test_day_summary_from_xml_inits_ok(summary_xml):
    summary_id = "summary_id"
    entry_mock = mock.Mock(spec=DaySummaryEntry)
    root = ElementTree.fromstring(summary_xml)

    with mock.patch("boedb.diario_boe.models.DaySummaryEntry") as DaySummaryEntryMock:
        DaySummaryEntryMock.return_value = entry_mock
        summary = DaySummary.from_xml(root)

    assert summary.summary_id == summary_id
    assert summary.publication_date == datetime(2023, 9, 14)
    assert summary.items == [entry_mock]
    assert summary.metadata == {"fecha": "14/09/2023"}


def test_day_summary_serializes_to_dict():
    entry_mock = mock.Mock(spec=DaySummaryEntry)
    metadata = {"fecha": "14/09/2023"}
    summary = DaySummary("summary_id", metadata, [entry_mock])

    assert summary.as_dict() == {
        "id": "summary_id",
        "publication_date": datetime(2023, 9, 14),
        "metadata": metadata,
    }


def test_article_inits_ok(article_data):
    article = Article.from_xml(article_data)
    assert article.article_id == "BOE-A-2023-18664"
    assert article.publication_date == datetime(2023, 8, 26)
    assert (
        article.title
        == """Resolución de 5 de agosto de 2023, conjunta de las Subsecretarías de Trabajo y Economía Social y de Inclusión, Seguridad Social y Migraciones, por la que se resuelve parcialmente la convocatoria de libre designación, efectuada por Resolución de 24 de marzo de 2023."""
    )
    assert article.content.startswith(
        """<p class="parrafo">Por Resolución de 24 de marzo de 2023"""
    )


def test_article_with_multiple_text_inits_ok(article_mtext_data):
    article = Article.from_xml(article_mtext_data)
    assert article.article_id == "BOE-A-2023-19658"
    assert article.publication_date == datetime(2023, 9, 19)
    assert (
        article.title
        == """Circular 1/2023, de 30 de agosto, de la Dirección General de Seguros y Fondos de Pensiones, relativa al uso obligatorio de medios electrónicos para la práctica de comunicaciones y notificaciones con los mediadores de seguros, corredores de reaseguros y determinados mediadores de seguros complementarios."""
    )
    assert article.content.startswith(
        """<p class="parrafo">La regulación de los distribuidores de seguros"""
    )


def test_article_splits_produces_articles(article_data):
    article = Article.from_xml(article_data)
    fragment_contents = ["text one", "text two"]
    with mock.patch.object(Article, "_split_text", return_value=fragment_contents):
        fragments = article.split()

    assert fragments[0].article_id == fragments[1].article_id == article.article_id
    assert fragments[0].metadata == fragments[1].metadata == article.metadata
    assert fragments[0].content, fragments[1].content == fragment_contents
    assert (fragments[0].sequence, fragments[0].total) == (1, 2)
    assert (fragments[1].sequence, fragments[1].total) == (2, 2)


def test_article_splits_doesnt_split_below_max(article_data):
    article = Article.from_xml(article_data)
    fragments = article.split(max_length=1e6)
    assert len(fragments) == 1


def test_article_splits_on_break():
    content = textwrap.dedent(
        """
        <p>total_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        <p>Artículo 1.</p>
        <p>total_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        """
    )
    metadata = {
        "fecha_publicacion": "20230921",
        "titulo": "test",
    }
    article = Article("test-id", metadata, content)
    fragments = article.split(max_length=200)
    assert len(fragments) == 2
    assert fragments[1].content.startswith("<p>Artículo 1.</p>")


def test_article_splits_on_middle_paragraph():
    content = textwrap.dedent(
        """
        <p>Aotal_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        <p>Botal_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        <p>total_length_of_the_line_is_50_characters</p>
        """
    )
    metadata = {
        "fecha_publicacion": "20230921",
        "titulo": "test",
    }
    article = Article("test-id", metadata, content)
    fragments = article.split(max_length=200)
    assert len(fragments) == 2
    assert fragments[1].content.startswith("\n<p>B")


def test_article_splits_content_by_half():
    content = textwrap.dedent(
        """
        <p>Aotal_length_of_the_line_is_50_charactersxxx
        xxxtotal_length_of_the_line_is_50_charactersxxx
        xxxtotal_length_of_the_line_is_50_charactersxxx
        ###total_length_of_the_line_is_50_charactersxxx
        xxxtotal_length_of_the_line_is_50_charactersxxx
        xxxtotal_length_of_the_line_is_50_characters</p>
        """
    )
    metadata = {
        "fecha_publicacion": "20230921",
        "titulo": "test",
    }
    article = Article("test-id", metadata, content)
    fragments = article.split(max_length=200)
    assert len(fragments) == 2
    assert fragments[1].content.startswith("###")

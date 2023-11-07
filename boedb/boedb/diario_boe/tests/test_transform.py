from unittest import mock

import pytest

from boedb.diario_boe.models import Article, ArticleFragment
from boedb.diario_boe.transform import ArticlesTransformer


def test_article_transformer_title_summary_prompt():
    transformer = ArticlesTransformer(1, http_session=mock.AsyncMock())

    test_title = "article title"
    article = mock.Mock(title=test_title)
    prompt = transformer.get_title_summary_prompt(article.title)

    assert type(prompt) is list
    assert all("role" in msg and "content" in msg for msg in prompt) is True
    assert test_title in prompt[-1]["content"]


def test_article_transformer_content_summary_prompt():
    transformer = ArticlesTransformer(1, http_session=mock.AsyncMock())

    content = "article content"
    article = mock.Mock(content=content)
    prompt = transformer.get_content_summary_prompt(article.content)

    assert type(prompt) is list
    assert all("role" in msg and "content" in msg for msg in prompt) is True
    assert content in prompt[-1]["content"]


@pytest.mark.asyncio
async def test_article_transformer_processes_article():
    session_mock = mock.Mock()
    metadata = {"fecha_publicacion": "20231103", "titulo": "title"}
    article = Article("article_id", "summary_id", metadata, "content")
    transformed = mock.Mock()
    transformer = ArticlesTransformer(1, session_mock)
    with mock.patch.object(transformer, "process_article") as process_mock:
        process_mock.return_value = transformed
        result = await transformer.process(article)

    assert result is transformed
    process_mock.assert_awaited_once_with(article)


@pytest.mark.asyncio
async def test_article_transformer_processes_fragment():
    session_mock = mock.Mock()
    fragment = ArticleFragment("article_id", "content", 1, 2)
    transformed = mock.Mock()
    transformer = ArticlesTransformer(1, session_mock)
    with mock.patch.object(transformer, "process_fragment") as process_mock:
        process_mock.return_value = transformed
        result = await transformer.process(fragment)

    assert result is transformed
    process_mock.assert_awaited_once_with(fragment)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.transform.DiarioBoeConfig.TITLE_SUMMARIZATION_MIN_LENGTH", 1)
async def test_article_transformer_transforms_article():
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()
    prompt_mock = mock.Mock()

    test_summary = "title summary"
    test_embedding = [0.1, 0.2]

    metadata = {"fecha_publicacion": "20231103", "titulo": "a_title_of_29_characters_long"}
    article = Article("article_id", "summary_id", metadata, "content")

    with mock.patch("boedb.diario_boe.transform.OpenAiClient") as OpenAiClientMock, mock.patch(
        "boedb.diario_boe.transform.ArticlesTransformer.get_title_summary_prompt", return_value=prompt_mock
    ):
        OpenAiClientMock.return_value = client_mock
        client_mock.complete.return_value = test_summary
        client_mock.get_embeddings.return_value = test_embedding

        transformer = ArticlesTransformer(1, session_mock)
        transformed = await transformer.process(article)

    assert transformed is article
    assert transformed.title_summary == test_summary
    assert transformed.title_embedding == test_embedding

    client_mock.complete.assert_awaited_once_with(prompt_mock, max_tokens=14)
    client_mock.get_embeddings.assert_awaited_once_with(article.title)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.transform.DiarioBoeConfig.TITLE_SUMMARIZATION_MIN_LENGTH", 200)
async def test_article_transformer_transforms_article_with_short_title():
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()

    metadata = {"fecha_publicacion": "20231103", "titulo": "a_short_title"}
    article = Article("article_id", "summary_id", metadata, "content")

    with mock.patch("boedb.diario_boe.transform.OpenAiClient") as OpenAiClientMock:
        OpenAiClientMock.return_value = client_mock
        transformer = ArticlesTransformer(1, session_mock)
        transformed = await transformer.process(article)

    assert transformed is article
    assert transformed.title_summary == article.title
    client_mock.complete.assert_not_awaited()
    client_mock.get_embeddings.assert_awaited_once_with(article.title)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.transform.DiarioBoeConfig.CONTENT_SUMMARIZATION_MIN_LENGTH", 1)
async def test_article_transformer_transforms_fragment():
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()
    prompt_mock = mock.Mock()

    test_summary = "test_summary"
    test_embedding = [0.1, 0.2]

    fragment = ArticleFragment("article_id", "a_content_with_33_characters_long", 1, 2)

    with mock.patch("boedb.diario_boe.transform.OpenAiClient") as OpenAiClientMock, mock.patch(
        "boedb.diario_boe.transform.ArticlesTransformer.get_content_summary_prompt", return_value=prompt_mock
    ):
        OpenAiClientMock.return_value = client_mock
        client_mock.complete.return_value = test_summary
        client_mock.get_embeddings.return_value = test_embedding

        transformer = ArticlesTransformer(1, session_mock)
        transformed = await transformer.process(fragment)

    assert transformed is fragment
    assert transformed.summary == test_summary
    assert transformed.embedding == test_embedding
    client_mock.complete.assert_awaited_once_with(prompt_mock, max_tokens=11)
    client_mock.get_embeddings.assert_awaited_once_with(fragment.content)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.transform.DiarioBoeConfig.CONTENT_SUMMARIZATION_MIN_LENGTH", 200)
async def test_article_transformer_transforms_fragment_with_short_content():
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()

    fragment = ArticleFragment("article_id", "a_content_with_33_characters_long", 1, 2)

    with mock.patch("boedb.diario_boe.transform.OpenAiClient") as OpenAiClientMock:
        OpenAiClientMock.return_value = client_mock
        transformer = ArticlesTransformer(1, session_mock)
        transformed = await transformer.process(fragment)

    assert transformed is fragment
    assert transformed.summary == fragment.content
    client_mock.complete.assert_not_awaited()
    client_mock.get_embeddings.assert_awaited_once_with(fragment.content)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.transform.DiarioBoeConfig.CONTENT_SUMMARIZATION_MIN_LENGTH", 1)
async def test_article_transformer_transforms_fragment_clean_content():
    session_mock = mock.Mock()
    client_mock = mock.AsyncMock()

    test_prompt = "test prompt"
    test_summary = "test_summary"
    test_embedding = [0.1, 0.2]

    clean_content = "clean_content_of_length_26"
    fragment = ArticleFragment("article_id", "clean <table>table</table> content", 1, 2)

    with mock.patch("boedb.diario_boe.transform.OpenAiClient") as OpenAiClientMock, mock.patch(
        "boedb.diario_boe.transform.ArticlesTransformer.get_content_summary_prompt"
    ) as prompt_mock, mock.patch("boedb.diario_boe.transform.HTMLFilter") as HTMLFilterMock:
        prompt_mock.return_value = test_prompt
        HTMLFilterMock.clean_html.return_value = clean_content

        OpenAiClientMock.return_value = client_mock
        client_mock.complete.return_value = test_summary
        client_mock.get_embeddings.return_value = test_embedding

        transformer = ArticlesTransformer(1, session_mock)
        transformed = await transformer.process(fragment)

    assert transformed is fragment
    assert transformed.summary == test_summary
    assert transformed.embedding == test_embedding
    HTMLFilterMock.clean_html.assert_called_once_with(fragment.content)
    prompt_mock.assert_called_once_with(clean_content)
    client_mock.complete.assert_awaited_once_with(test_prompt, max_tokens=8)
    client_mock.get_embeddings.assert_awaited_once_with(clean_content)

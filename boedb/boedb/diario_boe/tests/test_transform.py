from unittest import mock

import pytest

from boedb.diario_boe.transform import DiarioBoeArticleTransformer


def test_diario_boe_article_transformer_title_summary_prompt():
    transformer = DiarioBoeArticleTransformer(http_session=mock.AsyncMock())

    test_title = "article title"
    article = mock.Mock(title=test_title)
    prompt = transformer.get_title_summary_prompt(article.title)

    assert type(prompt) is list
    assert all("role" in msg and "content" in msg for msg in prompt) is True
    assert test_title in prompt[-1]["content"]


def test_diario_boe_article_transformer_content_summary_prompt():
    transformer = DiarioBoeArticleTransformer(http_session=mock.AsyncMock())

    content = "article content"
    article = mock.Mock(content=content)
    prompt = transformer.get_content_summary_prompt(article.content)

    assert type(prompt) is list
    assert all("role" in msg and "content" in msg for msg in prompt) is True
    assert content in prompt[-1]["content"]


@pytest.mark.asyncio
async def test_diario_boe_article_transformer_process_title():
    test_summary = "test summary"
    test_embedding = [0.1, 0.2, 0.3]

    article = mock.Mock(title="title", content="content")

    llm_mock = mock.AsyncMock()
    with mock.patch("boedb.diario_boe.transform.OpenAiClient", return_value=llm_mock):
        llm_mock.complete.return_value = test_summary
        llm_mock.get_embeddings.return_value = test_embedding

        transformer = DiarioBoeArticleTransformer(http_session=mock.AsyncMock())
        processed = await transformer.process(article)

    assert processed is article
    assert article.title_summary == test_summary
    assert article.title_embedding == test_embedding

    summary_prompt = transformer.get_title_summary_prompt(article.title)
    max_tokens = len(article.title) // 2
    llm_mock.complete.assert_any_await(summary_prompt, max_tokens=max_tokens)
    llm_mock.get_embeddings.assert_any_await(article.title)


@pytest.mark.asyncio
async def test_diario_boe_article_transformer_process_content():
    test_summary = "test summary"
    test_embedding = [0.1, 0.2, 0.3]

    article = mock.Mock(title="title", content="content")

    llm_mock = mock.AsyncMock()
    with mock.patch("boedb.diario_boe.transform.OpenAiClient", return_value=llm_mock):
        llm_mock.complete.return_value = test_summary
        llm_mock.get_embeddings.return_value = test_embedding

        transformer = DiarioBoeArticleTransformer(http_session=mock.AsyncMock())
        processed = await transformer.process(article)

    assert processed is article
    assert article.summary == test_summary
    assert article.embedding == test_embedding

    summary_prompt = transformer.get_content_summary_prompt(article.content)
    max_tokens = len(article.content) // 3
    llm_mock.complete.assert_any_await(summary_prompt, max_tokens=max_tokens)
    llm_mock.get_embeddings.assert_any_await(article.content)


@pytest.mark.asyncio
@mock.patch("boedb.diario_boe.transform.HTMLFilter")
async def test_diario_boe_article_transformer_cleans_html_content(HTMLFilterMock):
    test_content = "<h1>title</h1><table>table</table><p>content</p>"
    clean_content = "title\ncontent"
    article = mock.Mock(title="title", content=test_content)

    HTMLFilterMock.clean_html.return_value = clean_content
    llm_mock = mock.Mock(complete=mock.AsyncMock(), get_embeddings=mock.AsyncMock())

    with mock.patch.object(
        DiarioBoeArticleTransformer, "get_content_summary_prompt"
    ) as get_content_summary_prompt_mock, mock.patch("boedb.diario_boe.transform.OpenAiClient", return_value=llm_mock):
        transformer = DiarioBoeArticleTransformer(http_session=mock.AsyncMock())
        await transformer.process(article)

    HTMLFilterMock.clean_html.assert_called_once_with(test_content)
    get_content_summary_prompt_mock.assert_called_once_with(clean_content)
    llm_mock.get_embeddings.assert_any_call(clean_content)


@pytest.mark.asyncio
async def test_diario_boe_article_transformer_processes_in_batch():
    items = [None, None, None]
    transformer = DiarioBoeArticleTransformer(http_session=mock.AsyncMock())
    with mock.patch.object(transformer, "process_in_batch"):
        await transformer(items)
        transformer.process_in_batch.assert_awaited_once_with(items)

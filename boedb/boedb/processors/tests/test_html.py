from boedb.processors.html import HTMLFilter


def test_htmlfilter_extracts_text_only():
    content = """<h1>title</h1><p>content1</p><p>content2</p>"""

    text = HTMLFilter.clean_html(content)
    assert text == "title\ncontent1\ncontent2"


def test_htmlfilter_removes_tags():
    content = """
        <h1>title</h1>
        <table>table<p>table_content</p></table>
        <p>content</p>
    """

    text = HTMLFilter.clean_html(content)
    assert "table" not in text


def test_htmlfilter_removes_nested_tags():
    content = """
        <h1>title</h1>
        <p>
            <table>table<table>table2</table></table>
        </p>
    """

    text = HTMLFilter.clean_html(content)
    assert "table" not in text


def test_htmlfilter_indents_lists():
    content = """
        <p>content</p>
        <ul>
            <li>item1</li>
            <li><ul>
                <li>item2.1</li>
                <li>item2.2</li>
            </ul></li>
        <ul>
    """

    text = HTMLFilter.clean_html(content)
    assert "  - item1" in text
    assert "    - item2.1" in text

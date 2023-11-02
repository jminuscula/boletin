from html.parser import HTMLParser


class HTMLFilter(HTMLParser):
    """
    Extract text from HTML, with minimal processing:
      - `REMOVE_TAGS` are removed from the content entirely
      - `INDENT_TAGS` have an indented hyphen appended
      - `PARAGRAPH_TAGS` add a breakline
    """

    REMOVE_TAGS = {"table", "tr", "td"}
    INDENT_TAGS = {"ul", "li"}
    PARAGRAPH_TAGS = {"br", "p", "section", "article", "h1", "h2", "h3", "h4", "ul"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.text = ""
        self.indent = 0
        self.remove_elems = 0

    def handle_starttag(self, tag, attrs):
        if tag in HTMLFilter.REMOVE_TAGS:
            self.remove_elems += 1

        if tag in HTMLFilter.INDENT_TAGS:
            self.indent += 1
            self.text += f"\n{'  ' * self.indent}- "

        if tag in HTMLFilter.PARAGRAPH_TAGS:
            self.text += "\n"

    def handle_endtag(self, tag):
        if tag in HTMLFilter.REMOVE_TAGS:
            self.remove_elems -= 1

        if tag in HTMLFilter.INDENT_TAGS:
            self.indent -= 1

    def handle_data(self, data):
        if not self.remove_elems:
            self.text += data

    @classmethod
    def clean_html(cls, html):
        html_filter = cls()
        html_filter.feed(html)
        return html_filter.text.strip()

from xml.etree import ElementTree

import xmltodict


def node_children_to_dict(root):
    """Return all child nodes and their text as a dictionary."""
    return xmltodict.parse(ElementTree.tostring(root))[root.tag] or {}


def node_text_content(root):
    if len(root) == 0:
        return root.text
    return "\n\n".join([ElementTree.tostring(n, "unicode") for n in root])


def find_node_with_ancestors(root, tag):
    """Iterate over all `tag` elements in `root` with a list of
    their ancestors elements."""

    def _find(_root, ancestors):
        if _root.tag == tag:
            yield (_root, ancestors)
            return

        for child in _root.iterfind("./"):
            yield from _find(child, [_root] + ancestors)

    yield from _find(root, [])

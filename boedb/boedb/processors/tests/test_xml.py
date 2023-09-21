from xml.etree import ElementTree

import pytest

from boedb.processors.xml import (
    find_node_with_ancestors,
    node_children_to_dict,
    node_text_content,
)


@pytest.fixture
def xml_tree():
    return ElementTree.fromstring(
        """
        <xml>
            <level1 level1_attr="level1_attr_value">
                <level2>
                    <key id="node1">data</key>
                </level2>
                <key id="node2">data</key>
                <key id="node3">data</key>
            </level1>
            <key id="node4">data</key>
        </xml>
        """
    )


def test_node_children_to_dict_ok(xml_tree):
    data = node_children_to_dict(xml_tree)
    assert data == {
        "level1": {
            "@level1_attr": "level1_attr_value",
            "level2": {"key": {"@id": "node1", "#text": "data"}},
            "key": [
                {"@id": "node2", "#text": "data"},
                {"@id": "node3", "#text": "data"},
            ],
        },
        "key": {"@id": "node4", "#text": "data"},
    }


def test_node_text_content_returns_all_children():
    xml = ElementTree.fromstring("<xml><one>data1</one><two>data2</two></xml>")
    text = node_text_content(xml)
    assert text == "<one>data1</one>\n\n<two>data2</two>"


def test_node_text_content_returns_all_if_no_children():
    xml = ElementTree.fromstring("<xml>data</xml>")
    text = node_text_content(xml)
    assert text == "data"


def test_find_node_with_ancestors_finds_items(xml_tree):
    node_ancestors = list(find_node_with_ancestors(xml_tree, "key"))
    assert all([node.tag == "key" for node, anc in node_ancestors])


def test_find_node_with_ancestors_returns_ancestors(xml_tree):
    node_ancestors = list(find_node_with_ancestors(xml_tree, "key"))
    node_ancestor_keys = (
        ["level2", "level1", "xml"],
        ["level1", "xml"],
        ["level1", "xml"],
        ["xml"],
    )

    for (node, ancestors), anc_keys in zip(node_ancestors, node_ancestor_keys):
        assert [a.tag for a in ancestors] == anc_keys

def extract_keys_with_metadata(root, extract_key, parent_key=None, meta=None):
    """Extract a dictionary object under a specific `extract_key`, along a metadata
    dictionary containing all the @attributes defined in the parent elements.
    """
    if meta is None:
        meta = {}

    if isinstance(root, list):
        for node in root:
            yield from extract_keys_with_metadata(node, extract_key, parent_key, meta)

    elif parent_key == extract_key:
        yield root, meta

    elif isinstance(root, dict):
        root_attrs = [attr for attr in root.keys() if attr.startswith("@")]
        root_meta = {attr: root[attr] for attr in root_attrs}
        if root_attrs:
            meta |= {parent_key: root_meta}

        for key, node in root.items():
            yield from extract_keys_with_metadata(node, extract_key, key, meta)

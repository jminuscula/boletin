from itertools import islice


def batched(iterable, n):  # pragma: no cover
    """Batch data into tuples of length n. The last batch may be shorter.
    https://docs.python.org/3/library/itertools.html
    """
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

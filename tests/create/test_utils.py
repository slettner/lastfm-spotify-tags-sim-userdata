from typing import List

import pytest

from lastfm_dataset.create.utils import nsplit


@pytest.mark.parametrize(
    ["arr", "n", "result"],
    [
        ([1, 2, 3, 4, 5, 6, 7, 8], 2, [[1, 2, 3, 4], [5, 6, 7, 8]]),
        ([1, 2, 3, 4, 5, 6, 7, 8], 3, [[1, 2, 3], [4, 5, 6], [7, 8]]),
        ([1, 2, 3, 4, 5, 6, 7, 8], 6, [[1, 2], [3, 4], [5], [6], [7], [8]]),
    ],
)
def test_nsplit(arr: List[int], n: int, result: List[List[int]]):
    assert list(nsplit(arr, n)) == result

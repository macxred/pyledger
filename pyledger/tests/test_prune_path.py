"""Test suite for prune_path() method."""

import pytest
import pandas as pd
from pyledger.helpers import prune_path


@pytest.mark.parametrize(
    "path, description, n, expected_path, expected_desc",
    [
        # Absolute paths
        ("/a/b/c/d", "original", 2, "/a/b", "c"),
        ("/a/b/c/", "desc", 1, "/a", "b"),
        ("/a/b", "fallback", 1, "/a", "b"),
        ("/only", "fallback", 1, "/only", "fallback"),

        # Relative paths
        ("foo/bar/baz", "something", 1, "/foo", "bar"),
        ("foo", "desc", 1, "/foo", "desc"),
        ("", "desc", 1, "/", "desc"),
    ]
)
def test_prune_path(path, description, n, expected_path, expected_desc):
    result_path, result_desc = prune_path(path, description, n)
    assert result_path == expected_path
    assert result_desc == expected_desc


@pytest.mark.parametrize(
    "path, description, n, expected_desc",
    [
        ("/x/y/z", "fallback", 0, "x"),
        ("/a", "desc", 0, "a"),
        ("a/b", "fallback", 0, "a"),
    ]
)
def test_prune_path_returns_na_for_path(path, description, n, expected_desc):
    result_path, result_desc = prune_path(path, description, n)
    assert pd.isna(result_path)
    assert result_desc == expected_desc


def test_prune_path_path_is_na():
    result = prune_path(pd.NA, "desc", 1)
    assert pd.isna(result[0]) and result[1] == "desc"

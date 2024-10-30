"""Test suite for testing timed_cache decorator"""

import time
import os
from pyledger.decorators import timed_cache


# Define the method that reads from a file and uses a timed cache
@timed_cache(seconds=2)
def read_file_content(filepath):
    """Reads content from a file."""
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            return file.read()
    return ''


def test_cache_works_within_timeout(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, 'w') as f:
        f.write("File Content")

    result1 = read_file_content(filepath)
    assert result1 == "File Content"

    result2 = read_file_content(filepath)
    assert result1 == result2
    assert result2 == "File Content"


def test_cache_handles_different_arguments(tmp_path):
    filepath1 = tmp_path / "test_file1.txt"
    filepath2 = tmp_path / "test_file2.txt"

    with open(filepath1, 'w') as f1, open(filepath2, 'w') as f2:
        f1.write("Content 1")
        f2.write("Content 2")

    result1 = read_file_content(filepath1)
    assert result1 == "Content 1"

    result2 = read_file_content(filepath2)
    assert result2 == "Content 2"


def test_manual_invalidation(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, 'w') as f:
        f.write("Initial Content")

    result1 = read_file_content(filepath)
    assert result1 == "Initial Content"

    with open(filepath, 'w') as f:
        f.write("Updated Content")

    result2 = read_file_content(filepath)
    assert result1 == result2

    read_file_content.cache_clear()

    result3 = read_file_content(filepath)
    assert result3 == "Updated Content"


def test_cache_expires_after_timeout(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, 'w') as f:
        f.write("Initial Content")

    result1 = read_file_content(filepath)
    assert result1 == "Initial Content"

    with open(filepath, 'w') as f:
        f.write("Updated Content")

    result2 = read_file_content(filepath)
    assert result1 == result2

    time.sleep(3)

    result3 = read_file_content(filepath)
    assert result3 == "Updated Content"


def test_cache_with_immediate_timeout(tmp_path):
    filepath = tmp_path / "test_file.txt"
    with open(filepath, 'w') as f:
        f.write("Immediate Cached Content")

    @timed_cache(seconds=0)
    def immediate_expiring_read(filepath):
        if os.path.exists(filepath):
            with open(filepath, 'r') as file:
                return file.read()
        return ''

    result1 = immediate_expiring_read(filepath)
    assert result1 == "Immediate Cached Content"

    os.remove(filepath)
    time.sleep(1)

    result2 = immediate_expiring_read(filepath)
    assert result2 == ''

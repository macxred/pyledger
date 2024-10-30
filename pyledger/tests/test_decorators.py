"""Test suite for timed_cache decorator"""

import time
from pyledger.decorators import timed_cache


# Define the method that reads from a file and uses a timed cache
@timed_cache(seconds=2)
def read_file_content(file):
    return file.read_text()


def test_cache_works(tmp_path):
    file = tmp_path / "test_file.txt"

    file.write_text("File Content")
    assert read_file_content(file) == "File Content"

    file.unlink()
    assert read_file_content(file) == "File Content"


def test_cache_handles_different_arguments(tmp_path):
    file1 = tmp_path / "test_file1.txt"
    file2 = tmp_path / "test_file2.txt"

    file1.write_text("Content 1")
    file2.write_text("Content 2")
    assert read_file_content(file1) == "Content 1"
    assert read_file_content(file2) == "Content 2"

    file1.unlink()
    file2.unlink()
    assert read_file_content(file1) == "Content 1"
    assert read_file_content(file2) == "Content 2"


def test_manual_invalidation(tmp_path):
    file = tmp_path / "test_file.txt"

    file.write_text("Initial Content")
    assert read_file_content(file) == "Initial Content"

    file.write_text("Updated Content")
    assert read_file_content(file) == "Initial Content"

    read_file_content.cache_clear()
    assert read_file_content(file) == "Updated Content"


def test_cache_expires_after_timeout(tmp_path):
    file = tmp_path / "test_file.txt"

    file.write_text("Initial Content")
    assert read_file_content(file) == "Initial Content"

    file.write_text("Updated Content")
    assert read_file_content(file) == "Initial Content"

    time.sleep(2)
    assert read_file_content(file) == "Updated Content"


def test_cache_with_immediate_timeout(tmp_path):
    file = tmp_path / "test_file.txt"

    @timed_cache(seconds=0)
    def immediately_expiring_read(file):
        return file.read_text()

    file.write_text("Original Content")
    assert immediately_expiring_read(file) == "Original Content"

    file.write_text("Updated Content")
    assert immediately_expiring_read(file) == "Updated Content"

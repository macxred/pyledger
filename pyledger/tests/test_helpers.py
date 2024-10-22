import pytest
from pathlib import Path
from pyledger.helpers import write_dict_to_yml, read_dict_from_yml


def test_write_and_read_dict_to_from_yml(tmp_path):
    test_file = Path(tmp_path) / "test_settings.yml"
    SAMPLE_DATA = {"key": "value"}

    write_dict_to_yml(SAMPLE_DATA, test_file)
    assert test_file.exists(), "YAML file should exist after writing."

    result = read_dict_from_yml(test_file)
    assert result == SAMPLE_DATA, "Read data should match written data."


def test_read_dict_from_yml_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_dict_from_yml(Path(tmp_path / "non_existent_file.yml"))


def test_write_dict_to_yml_invalid_path(tmp_path):
    invalid_file_path = Path(tmp_path / "invalid_path/test_settings.yml")
    with pytest.raises(FileNotFoundError):
        write_dict_to_yml({}, invalid_file_path)

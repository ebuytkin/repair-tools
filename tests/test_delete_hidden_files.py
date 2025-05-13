import logging
from pathlib import Path
from unittest.mock import Mock
import pytest 
import repair_tools.delete_hidden_files as dh_files

@pytest.fixture
def good_package(tmp_path: Path):
    pkg = tmp_path.joinpath("123456")

    pm_folder = pkg.joinpath("data/PreservationMasters")
    pm_folder.mkdir(parents=True)
    sc_folder = pkg.joinpath("data/ServiceCopies")
    sc_folder.mkdir(parents=True)

    pm_filepath = pm_folder.joinpath("mym_123456_v01_pm.flac")
    pmjson_filepath = pm_folder.joinpath("mym_123456_v01_pm.json")

    sc_filepath = sc_folder.joinpath("mym_123456_v01_sc.mp4")
    scjson_filepath = sc_folder.joinpath("mym_123456_v01_sc.json")

    for file in [
        pm_filepath,
        pmjson_filepath,
        sc_filepath,
        scjson_filepath,
        (pkg / "bagit.txt"),
        (pkg / "manifest-md5.txt"),
    ]:
        file.write_bytes(b"some bytes for object")

    f_metadata = pkg.joinpath("tags")
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath("mym_123456_v01_pm.mkv.xml.gz")
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b"some bytes for metadata")

    return pkg

def test_package_has_hidden_file(good_package: Path, caplog):
    hidden_package = good_package
    folder = hidden_package / "data" / "folder"
    folder.mkdir()
    hidden_file = folder / ".DS_Store"
    hidden_file.touch()

    dh_files.get_hidden_files(hidden_package)

    assert not hidden_file.exists()

    log_msg = (
        f"Removing: {str(hidden_file)}"
    )

    # assert log_msg in caplog.text

def test_package_has_hidden_folder(good_package: Path, caplog):
    hidden_package = good_package
    hidden_folder = hidden_package / "data" / "folder" / ".hidden_f" 
    hidden_folder.mkdir(parents=True)

    dh_files.get_hidden_files(hidden_package)

    assert not hidden_folder.exists()

    log_msg = (
        f"Removing: {str(hidden_folder)}"
    )

    # assert log_msg in caplog.text
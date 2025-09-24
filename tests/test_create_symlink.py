import pytest
import os
import argparse
from pathlib import Path
from repair_tools import create_symlink

@pytest.fixture
def setup_test_dirs(tmp_path):
    """creates temporary source and target dirs structure."""
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"

    (source_dir / "folder_A" / "123456").mkdir(parents=True)
    (source_dir / "folder_B" / "nested" / "789012").mkdir(parents=True)

    (source_dir / "folder_A" / "not_a_match").mkdir()
    (source_dir / "12345").mkdir()  # name too short
    (source_dir / "a_file.txt").touch()

    target_dir.mkdir()

    return source_dir, target_dir


def test_find_ami_in_subtree(setup_test_dirs):
    """test worker function finds correct directory in subdirs."""
    source_dir, _ = setup_test_dirs
    results = create_symlink.find_ami_in_subtree(source_dir / "folder_A")
    
    assert len(results) == 1
    assert results[0].name == "123456"
    assert isinstance(results[0], Path) # check it returns Path 

def test_get_source_dirs_parallel(setup_test_dirs):
    """tests main parallel function finds all matching dirs."""
    source_dir, _ = setup_test_dirs
    results = create_symlink.get_source_dirs_parallel(source_dir)

    # use set to compare names (parallel execution order not guaranteed)
    found_names = {path.name for path in results}
    expected_names = {"123456", "789012"}

    assert found_names == expected_names

def test_create_sym_success(setup_test_dirs):
    """tests symbolic links are created correctly."""
    source_dir, target_dir = setup_test_dirs
    
    # get list of pkgs
    pkg_list = [
        source_dir / "folder_A" / "123456",
        source_dir / "folder_B" / "nested" / "789012"
    ]

    create_symlink.create_sym(pkg_list, target_dir)

    # verify links were created at correct source
    link1 = target_dir / "123456"
    link2 = target_dir / "789012"

    assert link1.is_symlink()
    assert link2.is_symlink()
    assert os.readlink(link1) == str(pkg_list[0])

def test_create_sym_skips_existing_file(mocker, setup_test_dirs):
    """tests function warns and skips if link name already exists."""
    source_dir, target_dir = setup_test_dirs
    pkg_list = [source_dir / "folder_A" / "123456"]

    # create conflicting file in target directory
    (target_dir / "123456").touch()
    
    mock_log_warning = mocker.patch("logging.warning")

    create_symlink.create_sym(pkg_list, target_dir)

    mock_log_warning.assert_called_once_with("Symlink '123456' already exists. Skipping.")
    assert not (target_dir / "123456").is_symlink()

def test_create_sym_handles_permission_error(mocker, setup_test_dirs):
    """tests function logs error & stops on permission issue."""
    source_dir, target_dir = setup_test_dirs
    pkg_list = [source_dir / "folder_A" / "123456"]

    # make target directory read-only to cause PermissionError
    target_dir.chmod(0o555) # r&e permissions only

    mock_log_error = mocker.patch("logging.error")

    create_symlink.create_sym(pkg_list, target_dir)

    mock_log_error.assert_called_once()
    assert "Permission denied" in mock_log_error.call_args[0][0]

    # clean up permissions
    target_dir.chmod(0o755)


def test_extant_dir_validator(tmp_path):
    """tests valid directory confirmation."""
    # test w/ valid directory
    assert create_symlink.extant_dir(str(tmp_path)) == tmp_path

    # test w/ path to file
    file_path = tmp_path / "a_file.txt"
    file_path.touch()
    with pytest.raises(argparse.ArgumentTypeError):
        create_symlink.extant_dir(str(file_path))
    
    # test w/ non-existent path
    with pytest.raises(argparse.ArgumentTypeError):
        create_symlink.extant_dir(str(tmp_path / "non_existent"))

def test_main_flow(mocker):
    """tests main functions orchestration logic"""
    # mock all functions in main - test flow control
    mock_parse_args = mocker.patch("repair_tools.create_symlink.parse_args")
    mock_get_dirs = mocker.patch("repair_tools.create_symlink.get_source_dirs_parallel")
    mock_create_sym = mocker.patch("repair_tools.create_symlink.create_sym")
    mock_log_info = mocker.patch("logging.info")

    # mock return values
    mock_args = argparse.Namespace(source="fake/source", target="fake/target")
    mock_parse_args.return_value = mock_args
    mock_get_dirs.return_value = ["dir1", "dir2"]

    create_symlink.main()

    # assert functions were called in correct order w/ correct args
    mock_parse_args.assert_called_once()
    mock_log_info.assert_any_call("Finding AMI packages:")
    mock_get_dirs.assert_called_once_with("fake/source")
    mock_log_info.assert_any_call("Found 2 total directories.")
    mock_create_sym.assert_called_once_with(["dir1", "dir2"], "fake/target")
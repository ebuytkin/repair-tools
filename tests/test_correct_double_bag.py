from pathlib import Path
import pytest 
import repair_tools.correct_double_bag as cd_bag

@pytest.fixture
def bad_package(tmp_path: Path):
    pkg = tmp_path.joinpath("123456")

    pm_folder = pkg / "data" / "data" / "PreservationMasters"
    pm_folder.mkdir(parents=True)
    sc_folder = pkg / "data" / "data" / "ServiceCopies"
    sc_folder.mkdir(parents=True)

    pm_filepath = pm_folder.joinpath("mym_123456_v01_pm.flac")
    pmjson_filepath = pm_folder.joinpath("mym_123456_v01_pm.json")

    sc_filepath = sc_folder.joinpath("mym_123456_v01_sc.mp4")
    scjson_filepath = sc_folder.joinpath("mym_123456_v01_sc.json")

    manifest_filepath = pkg / "manifest-md5.txt"
    manifest_filepath.write_text("a938ce2d981c7892aa074f386b179461  data/data/PreservationMasters/mao_324891_v01_pm.json\na23952adec5523c1260bb9d6ca80a145  data/data/ServiceCopies/mao_324891_v01_pm.mkv")

    tm_filepath = pkg.joinpath("tagmanifest-md5.txt")
    bag_filepath = pkg.joinpath("bagit.txt")

    for file in [
        pm_filepath,
        pmjson_filepath,
        sc_filepath,
        scjson_filepath,
        tm_filepath,
        bag_filepath,
    ]:
        file.write_bytes(b"some bytes for object")

    f_metadata = pkg.joinpath("tags")
    f_metadata.mkdir()

    metadata_filepath = f_metadata.joinpath("mym_123456_v01_pm.mkv.xml.gz")
    metadata_filepath.touch()
    metadata_filepath.write_bytes(b"some bytes for metadata")

    return pkg

def test_bad_package(bad_package: Path):
    for dir in bad_package.rglob('PreservationMasters'):
        assert "data/data/" in str(dir.absolute)

    for dir in bad_package.rglob('ServiceCopies'):
        assert "data/data/" in str(dir.absolute)

    for d in bad_package.iterdir():
        assert Path(d / 'tagmanifest-md5.txt').exists
    

def test_edit_manifest(bad_package: Path):
    cd_bag.edit_manifest(bad_package)

    for manifest in bad_package.rglob("manifest-md5.txt"):
        assert "data/data" not in manifest.read_text()
        assert "data/" in manifest.read_text()

def test_rm_tagmanifest(bad_package: Path):
    cd_bag.rm_tagmanifest(bad_package)

    assert "tagmanifest-md5.txt" not in bad_package.rglob('*')

def test_correct_dbl_bag(bad_package: Path):

    cd_bag.correct_dbl_bag(bad_package)

    for d in bad_package.iterdir():
        if not d.is_dir():
            continue
        else:
            for dir in d.iterdir():
                assert "data/data/" not in str(dir.absolute)
                if dir.is_dir():
                    assert "data" in str(dir.absolute)

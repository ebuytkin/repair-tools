import json
import logging
import argparse
import os
import msgpack
import re 
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

INDEX_PATH = Path("/Users/emileebuytkins/Documents/Buytkins_Programming/index_files/repo_index.msgpack")
INDEX_TXT_FILE = Path("/Users/emileebuytkins/Documents/Buytkins_Programming/index_files/repo_index.txt")

########## logging
def setup_logging(log_file: Path):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # file handler
    log_file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(str(log_file), mode='a')
    fh.setFormatter(log_file_formatter)
    logger.addHandler(fh)

    # console handler
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

########## parser
def extant_dir(p: str) -> Path:
    path = Path(p)
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"{path} is not a directory")

    return path

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        "-s",
        type=extant_dir,
        required=True,
        help="""Complete path to original pkgs""",
        )
    parser.add_argument(
        "--ami-id",
        "-ami",
        nargs='*',
        help="""Complete path to AMI pkg. If multiple, separate by space.""",
        )
    
    parser.add_argument(
        "--target",
        "-t",
        type=extant_dir,
        required=True,
        help="""Complete path to dir where symlink will be created""",
        )
    
    parser.add_argument(
        "--sym_false",
        "-sf",
        action="store_true",
        help="""Flag to turn off symlink production for debugging""",
        )

    return parser.parse_args()

def create_index(source_dir: Path) -> dict:
    """
    Creates a targeted index by efficiently walking the directory.
    This version is efficient because it "prunes" the search.
    """
    logging.info(f"Starting targeted index of {source_dir}...")
    ami_regex = re.compile(r"^\d{6}$")
    index_data = {}

    for root, dirnames, _ in tqdm(os.walk(source_dir), desc="Indexing Source"):
        dirs_to_prune = []
        for dirname in dirnames:
            if ami_regex.match(dirname):
                full_path = os.path.join(root, dirname)

                if dirname not in index_data:
                    index_data[dirname] = []
                index_data[dirname].append(full_path)
                dirs_to_prune.append(dirname)
        
        if dirs_to_prune:
            dirnames[:] = [d for d in dirnames if d not in dirs_to_prune]

    return index_data

def get_create_index(source_dir: Path, cache_path: Path = INDEX_PATH) -> dict:
    if cache_path.exists():
        logging.info(f"Loading index from: {cache_path}")
        with open(cache_path, "rb") as f:
            return msgpack.load(f)
    else:
        logging.info("No index found. Creating new index.")
        index = create_index(source_dir)
        logging.info(f"Saving index to cache: {cache_path}")
        with open(cache_path, "wb") as f:
            msgpack.dump(index, f)
        return index

def search_index(index_data: dict, ami_id: str = None) -> list[Path]:
    all_paths = []
    if ami_id:
        for id in ami_id:
            paths_str = index_data.get(id, [])
            all_paths.extend(paths_str)
    else:
        for path_list in index_data.values():
            all_paths.extend(path_list)
    return [Path(p) for p in all_paths]

def create_single_symlink(source_item: Path, dest_path: Path):
    link_path = dest_path / source_item.name
    try:
        if not source_item.exists():
            logging.warning(f"Source {source_item} does not exist. Skipping symlink.")
            return
        link_path.symlink_to(source_item, target_is_directory=True)
    except FileExistsError:
        logging.warning(f"Symlink '{link_path.name}' already exists. Skipping.")
    except OSError as e:
        logging.error(f"Error creating symlink for {source_item.name}: {e}")

def create_sym(pkg_list: list, dest_path: Path):
    logging.info(f"Creating {len(pkg_list)} symlinks.")
    with ThreadPoolExecutor() as executor:

        list(tqdm(executor.map(lambda p: create_single_symlink(p, dest_path), pkg_list), total=len(pkg_list), desc="Creating Symlinks"))
    logging.info("Finished creating symlinks.")

def main():
    log_dir = Path("sym_logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(log_file=Path(log_dir / "create_symlink.log"))

    args = parse_args()

    index_data = get_create_index(args.source, INDEX_PATH)

    source_list = search_index(index_data, args.ami_id)

    if not source_list:
        logging.warning("No matching directories found in the index. If multiple AMI ids, make sure they are separated by spaces not commas.")
        return



    logging.info(f"Found {len(source_list)} total directories from index:\n ") 

    index_txt = INDEX_TXT_FILE
    with open(index_txt, "w") as f:
        for item in source_list:
            print(f"{str(item).split('/')[-1]}\n")
            # pkg name instead of full path:
            # item = str(item).split("/")[-1]
            f.write(f"{item}\n")

    if not args.sym_false:
        create_sym(source_list, args.target)

if __name__ == "__main__":
    main()
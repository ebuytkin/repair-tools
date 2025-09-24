import argparse
import logging
import shutil
import json
from pathlib import Path

# SOURCE_PATH = Path("/Volumes/lpasync")
SOURCE_PATH = Path("/Volumes/Archivematica/2_fa_components/")

DESTINATION_PATH = Path("/Volumes/Archivematica/2_fa_components/_reingest/")

INDEX_CACHE_FILE = Path(f"./{SOURCE_PATH.name}_index.json")

DIRS_TO_FIND = Path("/Users/emileebuytkins//Documents/Buytkins_Programming/reingest.txt").read_text().splitlines()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s - %(message)s'
    )

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rebuild-index",
        action="store_true",
        help="Force the script to rebuild the source directory index."
    )
    return parser.parse_args()

def build_index(root_path: Path) -> dict:
    logging.info(f"Building index for '{root_path}'.")
    index = {} 
    for path in root_path.rglob('*'):
        if path.is_dir():
            dir_name = path.name
            if dir_name not in index:
                index[dir_name] = []
            index[dir_name].append(str(path))
    return index

def get_index(force_rebuild: bool = False) -> dict:
    if not force_rebuild and INDEX_CACHE_FILE.exists():
        with open(INDEX_CACHE_FILE, 'r') as f:
            return json.load(f)
    
    index = build_index(SOURCE_PATH)
    with open(INDEX_CACHE_FILE, 'w') as f:
        json.dump(index, f, indent=2)
    return index

def main():
    setup_logging()
    args = parse_args()

    if not SOURCE_PATH.is_dir():
        logging.error(f"'{SOURCE_PATH}' does not exist or is not a directory.")
        return
    if not DESTINATION_PATH.is_dir():
        logging.error(f"'{DESTINATION_PATH}' does not exist or is not a directory.")
        return

    directory_index = get_index(force_rebuild=args.rebuild_index)
    
    moved_count = 0
    unmoved_dirs = dict()
    dir_exists_unmoved = set()

    logging.info(f"Starting search for {len(DIRS_TO_FIND)} package names.")

    for dir_name_to_find in sorted(list(DIRS_TO_FIND)):
        
        found_paths_str = directory_index.get(dir_name_to_find, [])
        found_directories = [Path(p) for p in found_paths_str]

        if not found_directories:
            logging.warning(f"'{dir_name_to_find}' not found in the index.")
            unmoved_dirs[dir_name_to_find] = "Not found in index."
            continue

        if len(found_directories) > 1:
            logging.warning(f"Duplicate: Found multiple directories named '{dir_name_to_find}'.")
            for path in found_directories:
                logging.warning(f"  - Location: {path}")

        for source_dir_path in found_directories:
            destination_dir_path = DESTINATION_PATH / source_dir_path.name

            if destination_dir_path.exists():
                logging.error(
                    f"'{destination_dir_path}' already exists. "
                    f"Skipping: {source_dir_path}"
                )
                dir_exists_unmoved.add(dir_name_to_find)
                continue

            try:
                logging.info(f"Moving '{source_dir_path}' to '{destination_dir_path}'...")
                shutil.move(source_dir_path, destination_dir_path)
                logging.info(f"Successfully moved '{source_dir_path.name}'.")
                moved_count += 1
            except Exception as e:
                logging.error(f"Failed to move '{source_dir_path}'. Reason: {e}")
                unmoved_dirs[dir_name_to_find] = e

    logging.info("--- MOVE SUMMARY ---")
    logging.info(f"Successfully moved: {moved_count}")
    
    exist_count = len(dir_exists_unmoved)
    logging.info(f"Moved previously, skipped: {exist_count}")

    unmoved_count = len(unmoved_dirs)
    logging.info(f"Not moved due to error: {unmoved_count}")

    if unmoved_dirs:
        logging.info("List of directories not moved due to errors:")
        for pkg, error in unmoved_dirs.items():
            logging.info(f"    {pkg}: {error}")

if __name__ == "__main__":
    main()
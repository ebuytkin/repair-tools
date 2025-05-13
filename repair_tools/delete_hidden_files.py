import argparse
import logging
from pathlib import Path
import repair_tools.cli as cli

logging.basicConfig(level=logging.INFO)

def parse_args() -> argparse.Namespace:
    parser = cli.Parser()
    parser.add_packagedirectory()

def get_hidden_files(directory: Path):
    for dir in directory.rglob('.*'):
        if dir.is_dir():
            dir.rmdir()
            logging.info(f"Removing: {str(dir)}")
        else:
            dir.unlink()
            logging.info(f"Removing: {str(dir.name)}")


def main():
    # args = parse_args()
    directory = Path("/Volumes/lpasync/prsv_prod_ingest/has_hidden_files")

    get_hidden_files(directory)


if __name__ == "__main__":
    main()
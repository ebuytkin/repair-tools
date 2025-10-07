import argparse
import logging
from pathlib import Path
import repair_tools.cli as cli

logging.basicConfig(level=logging.INFO)

class ExtendUnique(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        items = getattr(namespace, self.dest, None)

        if items is None:
            items = set(values)
        elif isinstance(items, set):
            items = items.union(values)

        setattr(namespace, self.dest, items)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser

    parser.add_argument(
            "--directory",
            type=list_of_paths,
            dest="packages",
            action=ExtendUnique,
            help="path to a directory of packages",
        )
    
def list_of_paths(p: str) -> list[Path]:
    path = extant_dir(p)
    child_dirs = []
    for child in path.iterdir():
        if child.is_dir():
            child_dirs.append(child)

    if not child_dirs:
        raise argparse.ArgumentTypeError(f"{path} does not contain child directories")

    return child_dirs

def extant_dir(p: str) -> Path:
    path = Path(p)
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"{path} is not a directory")

    return path


def get_hidden_files(directory: Path):
    for dir in directory.rglob('.*'):
        if dir.is_dir():
            dir.rmdir()
            logging.info(f"Removing: {str(dir)}")
        else:
            dir.unlink()
            logging.info(f"Removing: {str(dir.name)}")


def main():
    args = parse_args()
    # directory = Path("/Volumes/lpasync/prsv_prod_ingest/has_hidden_files")
    directory = Path(args.directory)

    get_hidden_files(directory)


if __name__ == "__main__":
    main()
import argparse
import logging
import os
# import json
import boto3
import logging
from datetime import datetime
from pathlib import Path

################# Logging setup
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

################# Parser
def extant_dir(p: str) -> str:
    if not os.path.isdir(p):
        raise argparse.ArgumentTypeError(f"{p} is not a directory")
    return p

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--package",
        "-p",
        type=extant_dir,
        help="Path to the AMI package"
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=extant_dir,
        help="Path to directory of AMI packages"
    )
    parser.add_argument(
        "--bucket",
        "-b",
        type=str,
        required=True,
        help="AWS S3 bucket name"
    )
    parser.add_argument(
        "--profile",
        "-awsp",
        type=str,
        required=True,
        help="AWS S3 profile name"
    )
    # just use bucket name?
    # parser.add_argument(
    #     "--index",
    #     type=str,
    #     help="Path to JSON index of bucket contents"
    # )
    parser.add_argument(
        "--logpath",
        "-lp",
        type=extant_dir,
        required=True,
        help="""base path to directory where log file and target directory index will be created.""",
        )
    return parser.parse_args()

################# Variables

CACHE_PATH = Path("/Users/emileebuytkins/Documents/Buytkins_Programming/download_sc_logs_index/s3_index.json")
NUM_THREADS = os.cpu_count() or 4 

################# Functions
# def save_bucket_index(bucket_name, output_json_path):
#     """Index all objects in the S3 bucket and save to a JSON file."""
#     s3 = boto3.client('s3')
#     paginator = s3.get_paginator('list_objects_v2')
#     objects = []
#     for page in paginator.paginate(Bucket=bucket_name):
#         for obj in page.get('Contents', []):
#             objects.append(obj)
#     with open(output_json_path, 'w') as f:
#         json.dump(objects, f, indent=2)
#     print(f"S3 bucket index saved to {output_json_path}")


def find_pm_files(preservation_dir: Path) -> list:
    """return a list of filenames ending in .flac or .mkv in PreservationMasters dir"""
    found = []
    for root, _, files in os.walk(preservation_dir):
        for file in files:
            if file.endswith('.flac') or file.endswith('.mkv'):
                found.append(file)
    return found

def check_corresponding_mp4s(package_path):
    """returns a list of preservation file names that do NOT have a corresponding .mp4"""
    preservation_dir = Path(package_path) / 'data' / 'PreservationMasters'
    service_copies_dir = Path(package_path) / 'data' / 'ServiceCopies'
    missing_mp4s = []

    pm_files = find_pm_files(preservation_dir)
    for file in pm_files:
        base = Path(file).stem
        mp4_name = base + '.mp4'
        mp4_path = service_copies_dir / mp4_name
        if mp4_path.exists():
            logging.info(f"Found corresponding mp4 for {file}: {mp4_name}")
        else:
            missing_mp4s.append(file)
    return missing_mp4s


# def search_index(ami_id, index_path):
#     with open(index_path, 'r') as f:
#         index = json.load(f)
#     for obj in index:
#         if ami_id in obj['Key'] and obj['Key'].lower().endswith('.mp4'): 
#             return obj['Key']
#     return None

def search_s3(filename, bucket_name, aws_profile):
    """search the S3 bucket for a key that matches the given filename (with .mp4 extension)"""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    mp4_name = Path(filename).stem + '.mp4'
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(mp4_name):
                return key
    return None

def download_mp4_from_s3(bucket_name, key, dest_path):
    """"downloads service copy from s3 bucket to destination path"""
    s3 = boto3.client('s3')
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket_name, key, str(dest_path))

def process_package(package_path: Path, bucket_name):
    missing_mp4s = check_corresponding_mp4s(package_path)
    if not missing_mp4s:
        print("All corresponding mp4s exist.")
        return

    service_copies_dir = package_path / 'data' / 'ServiceCopies'
    service_copies_dir.mkdir(parents=True, exist_ok=True)

    for pm_file in missing_mp4s:
        base = Path(pm_file).stem
        mp4_name = base + '.mp4'
        print(f"Searching S3 for {mp4_name}...")
        mp4_key = search_s3(base, bucket_name)
        if mp4_key:
            dest_path = service_copies_dir / mp4_name
            print(f"Downloading {mp4_key} from S3 bucket {bucket_name} to {dest_path}")
            download_mp4_from_s3(bucket_name, mp4_key, dest_path)
        else:
            print(f"No matching mp4 found in S3 for {mp4_name}.")

def main():
    log_path = Path(args.logpath)
    if not log_path.exists():
        log_path.mkdir(parents=True, exist_ok=True)
    setup_logging(log_file=Path(log_path / f"dowload_sc_{datetime.datetime.now().strftime('%Y%m%d')}.log"))

    args = parse_args()

    package_dir = Path(args.directory)
    package_path = Path(args.package)
    bucket_name = args.bucket

    aws_profile = args.profile
    boto3.setup_default_session(profile_name=aws_profile)

    if args.directory:
        for package in package_dir.iterdir():
            if package.is_dir():
                print(f"\nProcessing package: {str(package)}")
                process_package(package, bucket_name, aws_profile)
    # If --package is provided, process just that package
    elif args.package:
        process_package(package_path, bucket_name, aws_profile)
    else:
        print("No package or directory provided.")
        return


if __name__ == "__main__":
    main()
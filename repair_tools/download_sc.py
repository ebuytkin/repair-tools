import argparse
import logging
import os
import re
import logging
import boto3
import datetime
import json
import shutil
from pathlib import Path
from multiprocessing import Pool

import repair_tools.video_processing as vp

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
        help="Path to the AMI package missing service copies"
    )
    parser.add_argument(
        "--directory",
        "-d",
        type=extant_dir,
        help="Path to directory of AMI packages missing service copies"
    )
    parser.add_argument(
        "--bucket",
        "-b",
        type=str,
        help="AWS S3 bucket name to search in for matching .mp4 files"
    )
    parser.add_argument(
        "--profile",
        type=str,
        help="AWS S3 profile name"
    )
    parser.add_argument(
        "--test",
        action='store_true',
        help="flag to mock S3 interactions for testing purposes"
    )
    # just use bucket name?
    parser.add_argument(
        "--index",
        action='store_true',
        help="Path to JSON index of bucket contents"
    )

    return parser.parse_args()

################# Variables

CACHE_PATH = Path("/Users/emileebuytkins/Documents/Buytkins_Programming/download_sc_logs_index/s3_index.json")
NUM_THREADS = os.cpu_count() or 6

################# Functions

def find_pm_files(pkg_dir: Path) -> list:
    """return a list of filenames ending in .flac or .mkv in PreservationMasters dir"""
    pm_dir = pkg_dir / 'data' / 'PreservationMasters'
    found_pm_files = []
    v_filetypes = ('.mkv', '.mov', '.dv')
    a_filetypes = ('.flac', '.wav')
    for filetype in v_filetypes and a_filetypes:
        found_pm_files.extend(pm_dir.rglob(f'*{filetype}'))
    return found_pm_files

def create_sc(pkg_path: Path):
    """create service copy file & directory structure"""
    service_copies_dir = Path(pkg_path) / 'data' / 'ServiceCopies'
    service_copies_dir.mkdir(parents=True, exist_ok=True)

    pm_files = find_pm_files(pkg_path)

    print(f"DEBUG: {pm_files}")

    for file in pm_files:
        print(file.name)
        file_name = file.name
        if file.is_symlink():
            print(file.resolve())
            # file = str(file.resolve()).replace('/source/', '/Volumes/')
            file = str(file.resolve())
        file_path = Path(file)
        # repo_path = Path('/Volumes/') / Path(*file.readlink().parts[2:])
        vp.convert_to_mp4(file_path, file_name, service_copies_dir, audio_pan="auto")
   
    return

def _process_pkg_worker(pkg_path_str: str):
    """pool for multiple pkgs, returns a tuple of (pkg_path_str, success_bool, error_message_or_empty)."""
    pkg_path = Path(pkg_path_str)
    try:
        create_sc(pkg_path)
        return (pkg_path_str, True, "")
    except Exception as e:
        # Return the exception text so the parent can log it.
        return (pkg_path_str, False, f"{type(e).__name__}: {e}")
    
def json_datetime_serializer(obj):
    """Custom JSON serializer for objects not serializable by default json code.
    Specifically handles datetime objects."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def save_bucket_index(bucket_name, output_json_path):
    """Index all objects in the S3 bucket and save to a JSON file."""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    objects = []
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            objects.append(obj)
    
    with open(output_json_path, 'w') as f:
        # Use the 'default' parameter to handle datetime objects
        json.dump(objects, f, indent=2, default=json_datetime_serializer)

    print(f"S3 bucket index saved to {output_json_path}")

def search_index(ami_id, index_path):
    with open(index_path, 'r') as f:
        index = json.load(f)
    for obj in index:
        if ami_id in obj['Key'] and obj['Key'].lower().endswith(Path(ami_id).suffix.lower()):
        # if ami_id in obj['Key'] and obj['Key'].lower().endswith('.mp4'): 
            return obj['Key']
    return None
    
def search_s3(filename, bucket_name):
    """search the S3 bucket for a key that matches the given filename (with .mp4 extension)"""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    mp4_name = Path(filename).stem
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(mp4_name):
                print(f"Found matching key: {key}")
                return key
    return None

def download_mp4_from_s3(bucket_name, key, dest_path):
    """"downloads service copy from s3 bucket to destination path"""
    s3 = boto3.client('s3')
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket_name, key, str(dest_path))

# def find_package_dirs(root_dir: Path, pattern: re.Pattern) -> list[Path]:
#     logging.info(f"Searching for package directories in {root_dir} matching pattern")
#     found_paths = []
#     for dirpath, dirnames, _ in os.walk(root_dir):
#         # Filter the list of directories at the current level
#         for dirname in dirnames:
#             if pattern.search(dirname):
#                 found_paths.append(Path(dirpath) / dirname)
#     return found_paths

def main():
    # aws sso login --profile {profile} if token expired

    args = parse_args()

    # log_path = Path(args.logpath)

    log_path = Path("Users/emileebuytkins/Documents/Buytkins_Programming/download_sc_logs_index")
    if not log_path.exists():
        log_path.mkdir(parents=True, exist_ok=True)
    setup_logging(log_file=Path(log_path / f"dowload_sc_{datetime.datetime.now().strftime('%Y%m%d')}.log"))

    downloaded_sc = {}
    missing_sc = {}

    if not (args.package or args.directory):
        logging.error("You must specify either --package or --directory.")
        return
    if args.package and args.directory:
        logging.error("--package and --directory cannot be used at the same time.")
        return

    pkg_paths = []
    if args.package:
        pkg_paths.append(Path(args.package))
    else: 
        dir_pattern = re.compile(r"^\d{6}$")
        logging.info(f"Scanning {args.directory} for package directories matching pattern")
        # pkg_paths = find_package_dirs(Path(args.directory), dir_pattern)
        pkg_paths = [dir for dir in Path(args.directory).rglob("*") if dir_pattern.search(dir.name)]

    if not pkg_paths:
        logging.warning("No package directories found to process.")
        return

    logging.info(f"Found {len(pkg_paths)} package(s) to process.")

    if args.bucket and args.profile:
        boto3.setup_default_session(profile_name=args.profile)
        if args.index:
            if not CACHE_PATH.exists():
                logging.info(f"Cache file {CACHE_PATH} not found. Creating new index.")
                save_bucket_index(args.bucket, CACHE_PATH)
        for pkg_path in pkg_paths:
            sc_dir = pkg_path / 'data' / 'ServiceCopies'
            sc_dir.mkdir(parents=True, exist_ok=True)
            logging.info(f"Processing package for S3 download (if not in test mode): {pkg_path.name}")
            
            pm_files = find_pm_files(pkg_path)
            if len(pm_files) > 1:
                logging.info(f"Found {len(pm_files)} preservation master files in {pkg_path.name}, skipping.")
                for pm_file in pm_files:
                    missing_sc[(pm_file.name, "Multiple PM files")] = Path(pkg_path)
                continue

            for pm_file in pm_files:
                mp4_name = (pm_file.with_suffix('.mp4').name).replace('pm', 'sc')
                logging.info(f"Searching S3 for {mp4_name}")
                if args.index:
                    key = search_index(mp4_name, CACHE_PATH)
                else:
                    key = search_s3(mp4_name, args.bucket)

                print(f"DEBUG: key={key}, mp4_name={mp4_name}, sc_dir={sc_dir}")
                
                if key:
                    dest_path = sc_dir / mp4_name
                    if not args.test:
                        try:
                            download_mp4_from_s3(args.bucket, key, dest_path)
                            downloaded_sc[pm_file.name] = Path(pkg_path)
                            logging.info(f"Downloaded {mp4_name} to {dest_path}")
                        except Exception as e:
                            logging.error(f"Failed to download {mp4_name} from S3: {e}")
                            
                    else:
                        logging.info(f"[TEST MODE] Would download {mp4_name} to {dest_path}")

                    # json_name = Path(mp4_name).with_suffix('.json').name
                    # logging.info(f"Searching for corresponding JSON file: {json_name}")
                    
                    # if args.index:
                    #     json_key = search_index(json_name, CACHE_PATH)
                    # else:
                    #     json_key = search_s3(json_name, args.bucket)

                    # if json_key:
                    #     json_dest_path = sc_dir / json_name
                    #     if not args.test:
                    #         try:
                    #             download_mp4_from_s3(args.bucket, json_key, json_dest_path)
                    #             logging.info(f"Downloaded {json_name} to {json_dest_path}")
                    #         except Exception as e:
                    #             logging.error(f"Failed to download {json_name} from S3: {e}")
                    #     else:
                    #         logging.info(f"[TEST MODE] Would download {json_name} to {json_dest_path}")
                    # else:
                    #     logging.warning(f"No matching JSON file found for {mp4_name}")
                else:
                    logging.warning(f"No matching S3 mp4 found for {mp4_name}, trying alternate name.")
                    mp4_name = mp4_name.replace('sc', 'em')
                    logging.info(f"Searching S3 for {mp4_name}")
                    if args.index:
                        key = search_index(mp4_name, CACHE_PATH)
                    else:
                        key = search_s3(mp4_name, args.bucket)

                    print(f"DEBUG: key={key}, mp4_name={mp4_name}, sc_dir={sc_dir}")

                    if key:
                        dest_path = sc_dir / mp4_name
                        if dest_path.exists():
                            logging.info(f"Service copy {dest_path} already exists, skipping download.")
                            downloaded_sc[pm_file.name] = Path(pkg_path)
                            continue
                        if not args.test:
                            try:
                                download_mp4_from_s3(args.bucket, key, dest_path)
                                downloaded_sc[pm_file.name] = Path(pkg_path)
                                logging.info(f"Downloaded {mp4_name} to {dest_path}")
                            except Exception as e:
                                logging.error(f"Failed to download {mp4_name} from S3: {e}")
                        else:
                            logging.info(f"[TEST MODE] Would download {mp4_name} to {dest_path}")
                        # json_name = Path(mp4_name).with_suffix('.json').name
                        # logging.info(f"Searching for corresponding JSON file: {json_name}")
                        
                        # if args.index:
                        #     json_key = search_index(json_name, CACHE_PATH)
                        # else:
                        #     json_key = search_s3(json_name, args.bucket)

                        # if json_key:
                        #     json_dest_path = sc_dir / json_name
                        #     if not args.test:
                        #         try:
                        #             download_mp4_from_s3(args.bucket, json_key, json_dest_path)
                        #             logging.info(f"Downloaded {json_name} to {json_dest_path}")
                        #         except Exception as e:
                        #             logging.error(f"Failed to download {json_name} from S3: {e}")
                        #     else:
                        #         logging.info(f"[TEST MODE] Would download {json_name} to {json_dest_path}")
                        # else:
                        #     logging.warning(f"No matching JSON file found for {mp4_name}")
                    else:
                        logging.warning(f"No matching S3 mp4 found for {mp4_name}")
                        missing_sc[pm_file.name] = Path(pkg_path)
    else:
        if args.test:
            logging.info("[TEST MODE] Skipping local conversion.")
            for pkg_path in pkg_paths:
                logging.info(f"[TEST MODE] Would process package: {pkg_path.name}")
            return

        logging.info(f"Processing {len(pkg_paths)} package(s) using {NUM_THREADS} worker(s)")

        with Pool(processes=NUM_THREADS) as pool:
            pkg_path_strs = [str(p) for p in pkg_paths]
            results = pool.map(_process_pkg_worker, pkg_path_strs)

        for pkg_path_str, success, err in results:
            if success:
                logging.info(f"Succeeded: {pkg_path_str}")
                downloaded_sc.append(str(pkg_path_str.name))
            else:
                logging.error(f"Failed: {pkg_path_str} -> {err}")
                missing_sc.append(str(pkg_path_str.name))
    print("Downloaded service copies for the following packages:")
    for pkg, path  in downloaded_sc.items():
        print(f" - {pkg}")
        print(f"   located at: {path}\n")
        try:
            fixed_dir = Path(path.parent.parent / '_fixed')
            shutil.move(path, fixed_dir)
            logging.info(f"   Moved fixed package {path} to {fixed_dir}.")
        except Exception as e:
            logging.error(f"Failed to move fixed package {path} to {fixed_dir}: {e}")
    if missing_sc:
        print("\nMissing service copies for the following packages:")
        for pkg, path in missing_sc.items():
            print(f" - {pkg}")
            print(f"   located at: {path}\n")

if __name__ == "__main__":
    main()
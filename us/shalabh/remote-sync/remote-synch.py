import configparser
import logging
import os
import queue  # Use a queue for thread-safe communication.
import subprocess  # For using aws s3 sync command
import threading
import time

import boto3
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# --- Configuration ---
CONFIG_FILE = '../../../resources/config.ini'


def load_config():
    """
    config.ini format:
    [AWS]
    aws_access_key_id = YOUR_AWS_ACCESS_KEY
    aws_secret_access_key = YOUR_AWS_SECRET_KEY
    region_name = YOUR_REGION
    s3_bucket = your-s3-bucket-name
    s3_prefix = your/s3/prefix/

    [Local]
    watch_folders = /path/to/folder1,/path/to/folder2
    sync_interval = 5
    :return:
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    return config


config = load_config()

AWS_ACCESS_KEY_ID = config['AWS']['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['AWS']['aws_secret_access_key']
AWS_REGION = config['AWS']['region_name']
S3_BUCKET = config['AWS']['s3_bucket']
S3_PREFIX = config['AWS'].get('s3_prefix', '')  # Default to no prefix if not specified
WATCH_FOLDERS = [folder.strip() for folder in config['Local']['watch_folders'].split(',')]
SYNC_INTERVAL = int(config['Local']['sync_interval'])
PERIODIC_SYNC_INTERVAL = int(config['Local']['periodic_sync_interval']) # seconds

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    filename='../../../logs/remote-sync-log.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class FileChangeEventHandler(FileSystemEventHandler):
    def __init__(self, sync_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sync_queue = sync_queue
        self.last_sync_time = 0  # Initialize the last sync time

    def on_created(self, event):
        if not event.is_directory:  # only handle file events.
            self.sync_queue.put("sync")

    def on_modified(self, event):
        if not event.is_directory:
            self.sync_queue.put("sync")

    def on_deleted(self, event):
        if not event.is_directory:
            self.sync_queue.put("sync")

    def on_moved(self, event):
        if not event.is_directory:
            self.sync_queue.put("sync")


def aws_s3_sync(local_folder):
    """
    Performs the AWS S3 sync operation using the CLI for maximum performance.
    """
    last_folder_name = os.path.basename(local_folder)
    s3_uri = f"s3://{S3_BUCKET}/{S3_PREFIX}{last_folder_name}"
    command = [
        "aws", "s3", "sync",
        local_folder,
        s3_uri,
        # "--delete",   Remove files in S3 that don't exist locally
        "--exclude", "*.tmp",  # Exclude common temporary files.
        "--exclude", "*.DS_Store",  # ignore mac os files
        "--exclude", "*.swp", # ignore linux swp files
        "--no-progress",
        # "--aws-access-key-id", AWS_ACCESS_KEY_ID,
        # "--aws-secret-access-key", AWS_SECRET_ACCESS_KEY,
        "--region", AWS_REGION
    ]

    try:
        logging.info(f"Syncing: {local_folder} to {s3_uri}")
        result = subprocess.run(command, capture_output=True, text=True,
                                check=True)  # check = True to raise exception for non 0 exit codes
        logging.info(f"Sync output:\n{result.stdout}")  # log sync output
        if result.stderr:
            logging.warning(f"Sync warnings/errors:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during sync: {e}\nStdout: {e.stdout}\nStderr: {e.stderr}")
    except FileNotFoundError:
        logging.error("aws command not found. Make sure the AWS CLI is installed and in your PATH.")
        exit(1)  # Critical error; exit.


def sync_worker(sync_queue):
    """
    Monitors the queue for sync requests and performs the synchronization.
    """
    last_sync_time = 0
    last_periodic_sync_time = 0  # Separate timer for periodic sync
    initial_sync_done = False

    while True:
        try:
            # Block until an item is available, or timeout.
            _ = sync_queue.get(timeout=SYNC_INTERVAL)
            current_time = time.time()
            # Throttle the sync operations.
            if current_time - last_sync_time >= SYNC_INTERVAL:
                for folder in WATCH_FOLDERS:
                    logging.info('Event Sync...')
                    aws_s3_sync(folder)
                last_sync_time = current_time
            else:
                # If a sync was requested, but it's too soon, just empty the queue
                while not sync_queue.empty():
                    sync_queue.get_nowait()

        except queue.Empty:
            current_time = time.time()
            # Throttle the sync operations.
            if not initial_sync_done:
                for folder in WATCH_FOLDERS:
                    logging.info('Initial Sync...')
                    aws_s3_sync(folder)
                last_sync_time = current_time
                last_periodic_sync_time = current_time # init periodic timer too
                initial_sync_done = True
            elif current_time - last_periodic_sync_time >= PERIODIC_SYNC_INTERVAL:
                # Perform periodic sync, independent of event-driven syncs
                for folder in WATCH_FOLDERS:
                    logging.info('Periodic Sync...')
                    aws_s3_sync(folder)
                last_periodic_sync_time = current_time  # Reset *only* the periodic timer

        except Exception as e:
            logging.error(f"Error in sync worker: {e}")


def main():
    sync_queue = queue.Queue()
    file_change_event_handler = FileChangeEventHandler(sync_queue)
    observers = []

    # Start the sync worker thread.
    sync_thread = threading.Thread(target=sync_worker, args=(sync_queue,), daemon=True)
    sync_thread.start()

    for folder in WATCH_FOLDERS:
        if not os.path.exists(folder):
            logging.error(f"Folder does not exist: {folder}")
            # exit(1)  # continue after logging, its not a critical error
        observer = Observer()
        observer.schedule(file_change_event_handler, folder, recursive=True)
        observer.start()
        observers.append(observer)
        logging.info(f"Monitoring folder: {folder}")

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive.
    except KeyboardInterrupt:
        for observer in observers:
            observer.stop()
    for observer in observers:
        observer.join()
    logging.info("Exiting.")


if __name__ == "__main__":
    main()

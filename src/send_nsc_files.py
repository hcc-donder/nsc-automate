"""
This script connects to an NSC FTP/SFTP server, retrieves files, and logs the operations.

Modules:
    os: Provides a way of using operating system dependent functionality.
    re: Provides regular expression matching operations.
    datetime: Supplies classes for manipulating dates and times.
    pathlib: Offers classes to handle filesystem paths.
    typing: Provides runtime support for type hints.
    csv: Implements classes to read and write tabular data in CSV format.
    paramiko: Implements the SSH2 protocol for secure (encrypted and authenticated) connections to remote machines.
    yaml: Provides YAML parsing and emitting capabilities.

Constants:
    CURRENT_DATETIME: The current date and time when the script is run.

These items need to be replaced with a more dynamic approach (see pycolleague for get_cfg function):
    IERG_PATH: The path to the IERG directory.
    NSC_PATH: The path to the NSC directory.
    DATA_PATH: The path to the Data directory.
    CONFIG_FILE: The path to the configuration file.

"""

# %% Initialize

import os
from pathlib import Path
from typing import Dict, Final, Optional
from datetime import datetime

import csv
import paramiko
import yaml
from paramiko import Transport
from pathlib import Path

from support_scripts import find_root, merge_dicts

# %% Constants
CURRENT_DATETIME: Final[datetime] = datetime.now()

# Get current datetime as YYYYMMDD_HHMMSS
CURRENT_DATETIME_STR: Final[str] = CURRENT_DATETIME.strftime("%Y%m%d_%H%M%S")

ROOT_PATH: Final[Path] = find_root("_IERG_SHARED_ROOT_DIR_")
NSC_PATH: Final[Path] = Path(ROOT_PATH / Path("nsc"))
DATA_PATH: Final[Path] = Path(ROOT_PATH / Path("Data"))
CONFIG_FILE: Final[Path] = Path(DATA_PATH / "config1.yml")
NSC_CONFIG_FILE: Final[Path] = Path(NSC_PATH / "nscconfig.yml")

def main():
    """
    Main function to load configuration, initialize log file, and connect to the NSC FTP/SFTP server.

    Steps:
    1. Load the configuration from the CONFIG_FILE.
    2. Initialize the log file if it does not exist.
    3. Open the log file for appending new log entries.
    """

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            ccdw_cfg: Dict = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        ccdw_cfg = {}

    try:
        with open(NSC_CONFIG_FILE, "r", encoding="utf-8") as file:
            nsc_cfg: Dict = yaml.load(file, Loader=yaml.FullLoader)
    except FileNotFoundError:
        nsc_cfg = {}

    # Merge the cfg and nsccfg dictionaries
    cfg = merge_dicts(ccdw_cfg, nsc_cfg)

    # Delete the partial dictionaries
    del ccdw_cfg
    del nsc_cfg
    send_path: str = cfg["nsc"]["ftp"]["send_path"]
    local_send_path: str = cfg["nsc"]["local"]["send_path"]
    archive_path: str = cfg["nsc"]["local"]["archive_path"]
    log_file = Path(cfg["nsc"]["local"]["log_file"])

    # Check if there are files to send
    files_to_send = [local_file for local_file in Path(local_send_path).iterdir() if local_file.is_file()]
    if not files_to_send:
        print("No files to send")
        return

    # Check if the log file exists and write headers if it does not
    if not log_file.exists():
        with open(log_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["file_name", "remote_file", "file_date_time", "status", "date_time"])

    # Open the log file for appending
    log_file_handle = open(log_file, mode='a', newline='', encoding='utf-8')
    log_writer = csv.writer(log_file_handle)

    # Connect using SFTP
    transport: Transport = paramiko.Transport((cfg["nsc"]["ftp"]["host"], int(cfg["nsc"]["ftp"]["port"])))
    transport.connect(username=cfg["nsc"]["ftp"]["username"], password=cfg["nsc"]["ftp"]["password"])
    sftp: Optional[paramiko.SFTPClient] = paramiko.SFTPClient.from_transport(transport)
    print("Connected to SFTP server")

    if sftp is not None:
        sftp.chdir(send_path)
    else:
        print("Failed to establish SFTP connection")
        return

    # Iterate over the list of files in the local send path
    for local_file in files_to_send:
        remote_file = send_path + '/' + local_file.name
        print(f"Uploading file: {local_file} to {remote_file}")
        sftp.put(local_file, remote_file)

        # Get the file's timestamp
        file_timestamp = datetime.fromtimestamp(local_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

        # Write the file information to the log file
        log_writer.writerow(
            [
                local_file.name,
                remote_file,
                file_timestamp,
                "Uploaded",
                CURRENT_DATETIME,
            ]
        )

        # Move the file to the archivePathwith the current date appended
        archive_file_name = f"{local_file.stem}_{CURRENT_DATETIME_STR}{local_file.suffix}"
        archive_file = Path(archive_path) / archive_file_name
        local_file.rename(archive_file)

    print("Done")
    log_file_handle.close()
    sftp.close()
    transport.close()

if __name__ == "__main__":
    main()

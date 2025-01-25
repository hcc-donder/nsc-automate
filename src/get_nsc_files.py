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
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Final, Optional

import csv
import paramiko

# import polars as pl
# import polars.selectors as cs
import yaml
from paramiko import Transport

from pathlib import Path

from support_scripts import find_root

####
# %% Constants
####
CURRENT_DATETIME: Final[datetime] = datetime.now()

# current_year: int = CURRENT_DATETIME.year
current_month: int = CURRENT_DATETIME.month

# Get current datetime as YYYYMMDD_HHMMSS
CURRENT_DATETIME_STR: Final[str] = CURRENT_DATETIME.strftime("%Y%m%d_%H%M%S")

ROOT_PATH: Final[Path] = find_root("_IERG_SHARED_ROOT_DIR_")
NSC_PATH: Final[Path] = Path(ROOT_PATH / Path("nsc"))
DATA_PATH: Final[Path] = Path(ROOT_PATH / Path("Data"))
CONFIG_FILE: Final[Path] = Path(DATA_PATH / "config1.yml")


def main():
    """
    Main function to load configuration, initialize log file, and connect to the NSC FTP/SFTP server.

    Steps:
    1. Load the configuration from the CONFIG_FILE.
    2. Initialize the log file if it does not exist.
    3. Open the log file for appending new log entries.
    """
    # Open the model.yml file and load the contents into the model variable
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        cfg: Dict = yaml.load(file, Loader=yaml.FullLoader)

    # Open log file as duckdb database
    log_file = Path(cfg["nsc"]["local"]["log_file"])
    if not log_file.exists():
        # Create the log file if it does not exist
        with open(log_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["nsc_file_name", "local_file_name", "file_date_time", "status", "date_time"])

    # Open the log file for appendingmain():
    # Open the model.yml file and load the contents into the model variable
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        cfg: Dict = yaml.load(file, Loader=yaml.FullLoader)

    # Open log file as duckdb database
    log_file = Path(cfg["nsc"]["local"]["log_file"])
    if not log_file.exists():
        # Create the log file if it does not exist
        with open(log_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["nsc_file_name", "local_file_name", "file_date_time", "status", "date_time"])

    # Open the log file for appending
    log_file_handle = open(log_file, mode='a', newline='', encoding='utf-8')
    log_writer = csv.writer(log_file_handle)

    # Get the current date and time for the date_time column in the log file
    current_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # %% Connect to NSC

    # Connect to the NSC ftp site using the credentials in the config file. The connection is stored in the nsc variable under ftp.
    # Use SFTP to connect to the NSC site.

    # Connect using SFTP
    transport: Transport = paramiko.Transport((cfg["nsc"]["ftp"]["host"], int(cfg["nsc"]["ftp"]["port"])))
    transport.connect(username=cfg["nsc"]["ftp"]["username"], password=cfg["nsc"]["ftp"]["password"])
    sftp: Optional[paramiko.SFTPClient] = paramiko.SFTPClient.from_transport(transport)
    print("Connected to SFTP server")

    receive_path: str = cfg["nsc"]["ftp"]["receive_path"]
    local_receive_path: str = cfg["nsc"]["local"]["receive_path"]
    if sftp is not None:
        sftp.chdir(receive_path)
    else:
        print("Failed to establish SFTP connection")
        return

    latest_file_time: Optional[float] = None
    new_latest_file_time: Optional[float] = None
    latest_file_path: Path = Path(local_receive_path) / "__Latest File Date"

    # Get the timestamp of the latest file if it exists
    if latest_file_path.exists():
        latest_file_time = latest_file_path.stat().st_mtime

    file_datetime: Optional[datetime] = None

    # Iterate over the list of files and their attributes
    for file_attr in sftp.listdir_attr():
        file_name: str = file_attr.filename
        if file_attr.st_mtime is not None:
            file_datetime = datetime.fromtimestamp(file_attr.st_mtime)
            

        # Skip files that are not newer than the latest file
        if (
            latest_file_time is not None
            and file_attr.st_mtime is not None
            and file_attr.st_mtime <= latest_file_time
        ):
            continue

        print(f"Downloading file: [{file_name}], Date and Time: {file_datetime}")

        # The file names on the FTP server are in the format of "dddddddd_dddddd_TYPE_MODE_MMDDYYYYHHMMSS_(PRE_)TTTTT(-TTTTT)_submitted.ext".
        # The characters represented by 'd' should be deleted from the resulting file name.
        # The TYPE section is one of: AGGRRPT, ANALYSISRDY, CNTLRPT, or DETLRPT.
        # The MODE section is one of: SE or PA.
        # The PRE section is optional and contains a prefix that should be saved as part of the file name (see below).
        # The TTTTT sections are term codes or term ids. The second one is optional. These should be saved as part of the file name.
        # The submitted section is text that should be saved as part of the file name.
        # The ext section is the file extension and will be either csv or htm.
        # The resulting local file name should be in the format of "TTTTT(-TTTTT)_TYPE_MODE_PRE_submitted.ext".

        # Use regular expression to extract parts of the file name
        # The pattern is broken down into named groups for clarity
        pattern = (
            r"^"  # Start of the string
            r".+_.+_"  # Skip the first two sections
            r"(?P<type>\w+)_"  # TYPE section
            r"(?P<mode>\w+)_"  # MODE section
            r"\d{8}\d{6}_"  # Date and time section ignored
            r"(?P<pre>\w+_)?"  # Optional PRE section
            r"(?P<term1>\w{5,7})-?"  # First term code
            r"(?P<term2>\w{5,7})?"  # Optional second term code
            r"_(?P<submitted>.*)\."  # Submitted section
            r"(?P<ext>\w+)$"  # File extension
        )
        match = re.match(pattern, file_name)
        if match:
            type_mode = match.group(1, 2)
            pre = match.group(3) or ""
            term_codes = match.group(4, 5)
            submitted = match.group(6)
            ext = match.group(7)

            # Construct the new file name
            local_file_name = f"{'-'.join(filter(None, term_codes))}_{'_'.join(type_mode)}_{pre}{submitted}.{ext}"
        else:
            local_file_name = file_name

        # Download the file and save it to cfg["nsc"]["local"]["receive_path"]
        local_file_path = Path(local_receive_path) / local_file_name
        sftp.get(file_name, local_file_path)

        # Fix the timestamp on the new file to match the timestamp on the remote file
        if file_attr.st_atime is not None and file_attr.st_mtime is not None:
            os.utime(local_file_path, (file_attr.st_atime, file_attr.st_mtime))
        else:
            print(
                f"Skipping timestamp update for {local_file_name} due to missing time attributes"
            )

        # Update the latest file time
        if new_latest_file_time is None or (
            file_attr.st_mtime is not None and file_attr.st_mtime > new_latest_file_time
        ):
            new_latest_file_time = file_attr.st_mtime


        # Write the file information to the log file
        log_writer.writerow(
            [
                file_name,
                local_file_path,
                file_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "Downloaded",
                current_date_time,
            ]
        )

    if new_latest_file_time is not None:
        latest_file_path.touch()
        os.utime(latest_file_path, (new_latest_file_time, new_latest_file_time))

    print("Done")
    log_file_handle.close()

if __name__ == "__main__":
    main()

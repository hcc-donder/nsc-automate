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

from support_scripts import find_root, merge_dicts

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
CONFIG_FILE: Final[Path] = Path(DATA_PATH / "config.yml")
NSC_CONFIG_FILE: Final[Path] = Path(NSC_PATH / "nscconfig.yml")

# The current path of this script file
SCRIPT_PATH: Final[Path] = Path(__file__).resolve().parent

def main():
    """
    Main function to load configuration, initialize log file, and connect to the NSC FTP/SFTP server.

    Steps:
    1. Load the configuration from the CONFIG_FILE.
    2. Initialize the log file if it does not exist.
    3. Open the log file for appending new log entries.
    """
    # Open the model.yml file and load the contents into the model variable
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

    added_base: Dict[str, str] = {}

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

        if added_base:
            # Need to remove all the base variables from the previous iteration
            for key in list(added_base.keys()):
                del globals()[key]
                added_base[key]

        # The file names on the FTP server are in the format of "CCCCCCCC_IIIIII_TYPE_MODE_MMDDYYYYHHMMSS_fn.ext".
        # The TYPE section is one of: AGGRRPT, ANALYSISRDY, CNTLRPT, or DETLRPT.
        # The MODE section is one of: DA, SE, or PA.

        # Use regular expression to extract parts of the file name
        # The pattern is broken down into named groups for clarity
        pattern = (
            r"^"  # Start of the string
            r"(?P<schoolcode>.+)_" # SCHOOL CODE
            r"(?P<idx>.+)_"
            r"(?P<nsctype>\w+)_"  # TYPE section
            r"(?P<nscmode>\w+)_"  # MODE section
            r"(?P<subdatetime>\d{8}\d{6})_"  # Date and time section
            r"(?P<fn>.+)\.(?P<ext>\w+)"  # File name and extension
            r"$"  # End of the string
        )
        

        match = re.match(pattern, file_name)
        if match:
            named_groups = match.groupdict()
            # Create global variables for each named group
            for key, value in named_groups.items():
                # print(f"Setting global variable {key} to {value}")
                globals()[key] = value
                added_base[key] = value

            subdatetime_dt: datetime = datetime.strptime(globals()["subdatetime"], "%m%d%Y%H%M%S") or datetime.today()

        import_cmd: str = ""
        added: Dict[str, str] = {}

        # Now, loop through the cfg["nsc"]["rename"] list to find a matching entry
        for rename_entry in cfg["nsc"]["rename"]:

            if added:
                # Need to remove all the variables added from the previous iteration
                for key in list(added.keys()):
                    del globals()[key]
                    del added[key]
                
            # If the modes equal, then we need to look at the pattern for a match
            if ("mode" in cfg["nsc"]["rename"][rename_entry] and globals()["nscmode"] == cfg["nsc"]["rename"][rename_entry]["mode"]):

                fn_match: Optional[re.Match] = None
                if ("pattern" in cfg["nsc"]["rename"][rename_entry] and (fn_match := re.match(cfg["nsc"]["rename"][rename_entry]["pattern"], globals()["fn"]))):
                    if fn_match:
                        print("Match found for :", rename_entry)
                        # get the named groups from the regex match and create varables for each named group
                        named_groups = fn_match.groupdict()
                        for key, value in named_groups.items():
                            globals()[key] = value
                            # Save value in dictionary "added"
                            added[key] = value
                    else:
                        continue

                # Construct the new file name based on the merged_cfg["nsc"]["rename"][rename_entry]["replace"]
                # The string in that entry is an f-string
                local_file_name: str = cfg["nsc"]["rename"][rename_entry]["replace"].format(**globals())

            else:
                # If no matching entry is found, use the original file name

                local_file_name = file_name

            if "local_file_path" in globals():
                del globals()["local_file_path"]
            globals()["local_file_path"] = Path(local_receive_path) / local_file_name

            if (globals()["nsctype"] == cfg["nsc"]["import"]["type"] and cfg["nsc"]["rename"][rename_entry]["import"]):
                # Get the import command from cfg["nsc"]["import"]
                import_cmd = f"""
                    python {cfg["nsc"]["import"]["cmd"].format(fn=local_file_path, 
                                                                  dt=CURRENT_DATETIME_STR,
                                                                  entry=rename_entry)}
                                                                  """.strip()
                # replace the './' with the path to the script
                import_cmd = import_cmd.replace("./", str(SCRIPT_PATH) + "/").replace("\\", "/")

        # Download the file and save it to cfg["nsc"]["local"]["receive_path"]
        sftp.get(file_name, globals()["local_file_path"])

        # Fix the timestamp on the new file to match the timestamp on the remote file
        if file_attr.st_atime is not None and file_attr.st_mtime is not None:
            os.utime(globals()["local_file_path"], (file_attr.st_atime, file_attr.st_mtime))
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

        # If an import command is specified, execute it
        if import_cmd:
            print(f"Running import command: {import_cmd}")
            # Check if os.system call was successful and log the result
            if os.system(import_cmd) == 0:
                print("Import successful")
                # Write the import information to the log file, if successful
                log_writer.writerow(
                    [
                        file_name,
                        local_file_path,
                        file_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "Imported",
                        current_date_time,
                    ]
                )
            else:
                print("Import failed")

    if new_latest_file_time is not None:
        latest_file_path.touch()
        os.utime(latest_file_path, (new_latest_file_time, new_latest_file_time))

    print("Done")
    log_file_handle.close()

if __name__ == "__main__":
    main()

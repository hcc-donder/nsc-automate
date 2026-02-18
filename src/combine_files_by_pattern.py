# %% Initialize

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Final, Optional

import csv


# import polars as pl
# import polars.selectors as cs
import yaml

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

# The current path of this script file
SCRIPT_PATH: Final[Path] = Path(__file__).resolve().parent

def main():
    pass

if __name__ == "__main__":
    main()
    
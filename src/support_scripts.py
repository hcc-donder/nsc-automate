from pathlib import Path
from typing import Dict

def find_root(file_name, start_path=None):
    """
    Recursively find the root directory containing a specific file.

    Args:
        file_name (str): The name of the file to search for.
        start_path (str or Path, optional): The starting directory. Defaults to the current working directory.

    Returns:
        Path: The Path object for the directory containing the file, or the current directory if not found.
    """
    start_path = Path(start_path or Path.cwd())
    for parent in start_path.parents:
        if (parent / file_name).exists():
            return parent
    return Path.cwd()

def merge_dicts(cfg1: Dict, cfg2: Dict) -> Dict:
    merged = cfg1.copy()  # Start with the values in cfg
    for key, value in cfg2.items():
        if isinstance(value, dict) and key in merged:
            merged[key] = merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged
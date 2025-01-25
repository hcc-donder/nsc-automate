import sys
import argparse
from datetime import datetime

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Import NSC file into the database.")
    parser.add_argument("entry", type=str, help="ID from the YAML file for the matched import statement")
    parser.add_argument("fn", type=str, help="Name of the file to be imported")
    parser.add_argument("dt", type=str, help="Date of the file in YYYYMMDD_HHMMSS format")

    # Parse arguments
    args = parser.parse_args()

    # Validate and parse the date argument
    try:
        file_date = datetime.strptime(args.dt, "%Y%m%d_%H%M%S")
    except ValueError:
        print("Error: Date format should be YYYYMMDD_HHMMSS")
        sys.exit(1)

    # Print the parsed arguments (for debugging purposes)
    print(f"Entry ID: {args.entry}")
    print(f"File Name: {args.fn}")
    print(f"File Date: {file_date}")

    # TODO: Add code to import the file into the database

if __name__ == "__main__":
    main()
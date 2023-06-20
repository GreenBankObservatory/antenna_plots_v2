import os
import argparse
from pathlib import Path

# argument: input directory
# loop through files of subdirectories
# execute all_antenna_data.py with file, destination as ~/repos/alda/alda/static/images/antenna_positions/session/

def generate_images(input_dir: Path | str, dest_dir: Path | str):
    parquet_list = Path(input_dir).glob("**/*/*.parquet")
    for path in parquet_list:
        os.system(f"python ~/repos/antenna_plots_v2/all_antenna_data.py {path} {Path(dest_dir) / path.stem}.parquet")
        print(f"Wrote {path.stem}.parquet")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=Path, help="The root directory containing subdirectories for sessions")
    parser.add_argument("dest_dir", type=Path, help="The destination directory for the images")
    args = parser.parse_args()
    return args.input_dir, args.dest_dir


def main():
    input_dir, dest_dir = parse_arguments()
    generate_images(input_dir, dest_dir)


if __name__ == "__main__":
    main()

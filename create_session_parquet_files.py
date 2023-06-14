import argparse
from pathlib import Path

from astropy.table import Table, vstack
import pandas as pd
import numpy as np


# df groupby
# default dict
# break up functions so easier to test


def group_files_df(manifest_path: Path | str):
    """Returns a pd dataframe with one column containing all the paths listed in the manifest file, and
    another column containing each file's associated session"""
    with open(manifest_path, "r") as file:
        rows = ((path.rstrip("\n"), Path(path).parent.parent.name) for path in file)
        return pd.DataFrame(rows, columns=["path", "session"])


def create_session_antenna_df(df, session):
    """Generates a dataframe containing the antenna position data for a given session"""
    sliced = df[df["session"] == session]
    table_list = []
    for path in sliced["path"]:
        table_list.append(Table.read(path, hdu=2)[["DMJD", "RAJ2000", "DECJ2000"]])
    return vstack(table_list).to_pandas()
        

def create_parquets(antenna_session_manifest_path: str, output_dir: str):
    """Generates a tree with output_dir as the root and subdirectories for each session, each containing
    a parquet file with combined antenna position data"""
    
    df = group_files_df(antenna_session_manifest_path)
    sessions = np.unique(df["session"])

    for session in sessions:

        session_dir = Path(output_dir, session)
        Path.mkdir(session_dir, parents=True, exist_ok=True)

        sliced_df = create_session_antenna_df(df, session)
        sliced_df.to_parquet(f"{Path(session_dir, session)}.parquet")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("antenna_session_manifest_path", type=Path, help="The files containing the paths to antenna FITS files")
    parser.add_argument("output_dir", type=Path, help="The created directory containing the tree of parquet files")
    args = parser.parse_args()
    manifest_path = str(args.antenna_session_manifest_path)
    output_dir = str(args.output_dir)
    return manifest_path, output_dir


def main():
    manifest_path, output_dir = parse_arguments()
    create_parquets(manifest_path, output_dir)


if __name__ == "__main__":
    main()

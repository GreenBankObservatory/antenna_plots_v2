import os
import argparse
import re

from astropy.table import Table, vstack
import pandas as pd
import numpy as np

# regular expression to match the session name given a path to antenna FITS file
SESSION_REGEX = re.compile(r"/[^/]*/Antenna")

def group_files_df(manifest_path: str):
    """Returns a pd dataframe with one column containing all the paths listed in the manifest file, and
    another column containing each file's associated session"""
    lines = open(manifest_path, "r").readlines()
    df = pd.DataFrame(lines, columns=["path"])
    df["path"] = df["path"].map(lambda x: x.replace("\n", ""))
    # get session name
    df["session"] = df["path"].map(lambda x: re.search(SESSION_REGEX, x).group(0).split("/")[1])
    return df


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
    
    os.makedirs(output_dir)

    df = group_files_df(antenna_session_manifest_path)
    sessions = np.unique(df["session"])

    for session in sessions:

        session_dir = os.path.join(output_dir, session)
        os.makedirs(session_dir)

        sliced_df = create_session_antenna_df(df, session)
        sliced_df.to_parquet(''.join([os.path.join(session_dir, session), ".parquet"]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("antenna_session_manifest_path", help="The files containing the paths to antenna FITS files")
    parser.add_argument("output_dir", help="The created directory containing the tree of parquet files")
    args = parser.parse_args()
    manifest_path = str(args.antenna_session_manifest_path)
    output_dir = str(args.output_dir)
    create_parquets(manifest_path, output_dir)


if __name__ == "__main__":
    main()

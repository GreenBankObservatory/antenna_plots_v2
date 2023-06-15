import argparse
from pathlib import Path
from collections import defaultdict

from astropy.table import Table, vstack


def group_files(manifest_path: Path | str):
    """Generates a dictionary grouping FITS file paths with their associated GBT session"""
    
    d: defaultdict[str, list[Path]] = defaultdict(list)
    with open(manifest_path, "r") as file:
        for path in file:
             d[Path(path).parent.parent.name].append(Path(path.rstrip("\n"))) 
    return dict(d)
        

def create_session_table(paths: list[Path | str]):
    """Combines antenna positions from a list of FITS files into one dataframe"""
    table_list = []
    for path in paths:
        table_list.append(Table.read(path, hdu=2, format='fits')[["DMJD", "RAJ2000", "DECJ2000"]])
    return vstack(table_list).to_pandas()


def create_parquets_dict(antenna_session_manifest_path: Path | str, output_dir: Path | str):
    """Generates a tree with output_dir as the root and subdirectories for each session, each containing
    a parquet file with combined antenna position data"""
    
    session_dict = group_files(antenna_session_manifest_path)
    
    for session in session_dict:

        session_dir = Path(output_dir, session)
        session_dir.mkdir(parents=True, exist_ok=True)

        sliced_df = create_session_table(session_dict[session])
        parquet_path = session_dir / f"{session}.parquet"
        sliced_df.to_parquet(parquet_path)
        print(f"Wrote {parquet_path}")


def parse_arguments():
    """Parses and returns arguments from the command line"""
    parser = argparse.ArgumentParser()
    parser.add_argument("antenna_session_manifest_path", type=Path, help="The files containing the paths to antenna FITS files")
    parser.add_argument("output_dir", type=Path, help="The created directory containing the tree of parquet files")
    args = parser.parse_args()
    manifest_path = args.antenna_session_manifest_path
    output_dir = args.output_dir
    return manifest_path, output_dir


def main():
    manifest_path, output_dir = parse_arguments()
    create_parquets_dict(manifest_path, output_dir)


if __name__ == "__main__":
    main()

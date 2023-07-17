from pathlib import Path
import argparse
from tqdm import tqdm

import numpy as np
from astropy.table import Table, Column, vstack, hstack

def stack_fits(input_dir, dest_path):
    table_list = []
    paths = list(Path(input_dir).glob("*/Antenna/*.fits"))
    for path in tqdm(paths):
        session = path.parts[-3]
        table = Table.read(path, hdu=2, format='fits')[["DMJD", "RAJ2000", "DECJ2000"]]
        table = hstack([
            table,
            Column(name="Session", data=np.full(len(table), session))
            # TODO: include observer, front/back ends, receiver
        ])
        table_list.append(table)
    df = vstack(table_list).to_pandas()
    df.to_parquet(f"{dest_path}.parquet")

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("dest_path", type=Path)
    args = parser.parse_args()
    return args.input_dir, args.dest_path

def main():
    input_dir, dest_path = parse_arguments()
    stack_fits(input_dir, dest_path)

if __name__ == "__main__":
    main()
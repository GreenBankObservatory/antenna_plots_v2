import argparse

import numpy as np
import pandas as pd
import astropy.units as u
from astropy.coordinates import SkyCoord, Angle
import geoviews as gv
from cartopy import crs


def project(df):
    projected = gv.operation.project_points(
        gv.Points(df, kdims=["GAL_LONG", "GAL_LAT"]).opts(
            gv.opts.Points(projection=crs.Mollweide(), global_extent=True)
        ),
        projection=crs.Mollweide(),
    ).data[["GAL_LONG", "GAL_LAT"]]
    projected = projected.rename(
        columns={"GAL_LONG": "projected_gal_long", "GAL_LAT": "projected_gal_lat"}
    )
    return projected


def add_gal_coords(input_data_path, output_path):
    print("Reading parquet...")
    df = pd.read_parquet(input_data_path)
    df = df.reset_index()

    print("Generating galactic coordinates...")
    eq = SkyCoord(
        df["RAJ2000"], df["DECJ2000"], unit=u.deg, frame="icrs", equinox="J2000"
    )
    gal = eq.galactic.to_table().to_pandas()

    print("Appending to dataframe...")
    df[["GAL_LONG", "GAL_LAT"]] = gal
    print(df)

    print("Wrapping...")
    wrapped_gal_long = Angle(np.array(df["GAL_LONG"]) * u.degree).wrap_at(
        180 * u.degree
    )
    df["GAL_LONG"] = wrapped_gal_long.value

    print("Projecting...")
    df = pd.concat([df, project(df)], axis=1)

    print("Setting and sorting index...")
    df = df.set_index(
        [
            "project",
            "session",
            "scan_start",
            "scan_number",
            "observer",
            "frontend",
            "backend",
            "procname",
            "obstype",
            "procscan",
            "proctype",
            "object",
            "script_name",
            "RAJ2000",
            "DECJ2000",
            "GAL_LONG",
            "GAL_LAT",
        ]
    ).sort_index()
    print(df)

    print("Saving parquet...")
    df.to_parquet(output_path)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_data_path")
    parser.add_argument("output_path")
    args = parser.parse_args()
    return args.input_data_path, args.output_path


def main():
    input_data_path, output_path = parse_arguments()
    add_gal_coords(input_data_path, output_path)


if __name__ == "__main__":
    main()

"""
Contains several functions: generates static images, projected points parquets, and dynamic plots
"""

import argparse
from pathlib import Path

from tqdm import tqdm

import datashader as ds
import pandas as pd
import holoviews as hv
import holoviews.operation.datashader as hd
hv.extension('bokeh')
import geoviews as gv
from cartopy import crs
import colorcet as cc


def remove_invalid_data(df: pd.DataFrame):
    """Removes data lying outside (0, 360) degrees RA and (-90, 90) degrees declination"""
    df = df[(df["RAJ2000"] >= 0) & (df["RAJ2000"] <= 360)]
    df = df[(df["DECJ2000"] >= -90) & (df["DECJ2000"] <= 90)]
    return df


def project(df: pd.DataFrame):
    """Returns projected geoviews points from an input dataframe of antenna positions"""
    points = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
    points = points.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=2000, height=1000))
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    return projected


def get_ranges():
    """Returns a table containing the projected ranges for right ascension and declination"""
    ranges_list = [[0, -90], [360, 90]]
    [ranges_list.append([i, 0]) for i in range(361)]
    ranges_df = pd.DataFrame(ranges_list, columns=["RAJ2000", "DECJ2000"])
    ranges_points = gv.Points(ranges_df, kdims=['RAJ2000', 'DECJ2000'])
    ranges_points = ranges_points.opts(gv.opts.Points(projection=crs.Mollweide()))
    ranges_projected = gv.operation.project_points(ranges_points, projection=crs.Mollweide())
    return ranges_projected.data


def generate_image(input_file: Path | str, dest: Path | str):
    """Reads a parquet file containing antenna positions and saves a static map of the data"""

    df = pd.read_parquet(input_file, columns=["DMJD", "RAJ2000", "DECJ2000"])
    df = remove_invalid_data(df)
    projected = project(df)
    ranges = get_ranges()
    canvas = ds.Canvas(plot_width=2000, plot_height=1000, 
                       x_range=(ranges["RAJ2000"].min(), ranges["RAJ2000"].max()),
                       y_range=(ranges["DECJ2000"].min(), ranges["DECJ2000"].max()))
    canvas_points = canvas.points(projected.data, "RAJ2000", "DECJ2000")
    ds_image = ds.tf.Image(ds.tf.set_background(ds.tf.shade(canvas_points, cc.rainbow4)))
    ds_image.to_pandas().to_parquet(dest)
    # ds_image.to_pil().save(dest)


def loop_generate_images(input_dir: Path | str, dest_dir: Path | str):
    """Reads from an antenna position parquet tree and generates a directory containing datashader Images"""
    parquet_list = list(Path(input_dir).glob("**/*/*.parquet"))
    for path in tqdm(parquet_list):
        generate_image(path, Path(dest_dir) / path.stem)


def save_projected_points(input_file: Path | str, dest: Path | str):
    """Reads a parquet file containing antenna positions and saves the projected geovies points
    as another parquet file"""
    df = pd.read_parquet(input_file, columns=["DMJD", "RAJ2000", "DECJ2000"])
    df = df[(df["RAJ2000"] >= 0) & (df["RAJ2000"] <= 360)]
    df = df[(df["DECJ2000"] >= -90) & (df["DECJ2000"] <= 90)]
    points = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    projected.data.to_parquet(dest)
    print(f"Wrote {dest}")


def loop_save_projected_points(input_dir: Path | str, dest_dir: Path | str):
    """Reads from an antenna position parquet tree and generates a directory containing projected
    geoviews points as parquet files"""
    path_list = list(Path(input_dir).glob("**/*/*.parquet"))
    for path in path_list:
        save_projected_points(path, f"{Path(dest_dir) / path.stem}.parquet")


def generate_projection_dynamic(input_file: Path | str, dest: Path | str):
    """Reads a parquet file containing antenna positions and saves a dynamic HTML plot of the data"""
    df = pd.read_parquet(input_file, columns=["DMJD", "RAJ2000", "DECJ2000"])
    df = remove_invalid_data(df)
    projected = project(df)
    shaded = hd.dynspread(hd.datashade(projected).opts(projection=crs.Mollweide(), global_extent=True, width=1000, height=500), threshold=1.0)
    hv.save(shaded, dest)


def loop_generate_projections_dynamic(input_dir: Path | str, dest_dir: Path | str):
    """Reads from an antenna position parquet tree and generates a directory containing HTML plots"""
    path_list = list(Path(input_dir).glob("**/*/*.parquet"))
    for path in tqdm(path_list):
        generate_projection_dynamic(path, f"{Path(dest_dir) / path.stem}.html")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path", type=Path)
    parser.add_argument("dest_path", type=Path)
    args = parser.parse_args()
    return args.input_path, args.dest_path


def main():
    input_path, dest_path = parse_arguments()
    # TODO: add arguments for different functions
    loop_generate_images(input_path, dest_path)
    # loop_generate_projections_dynamic(input_path, dest_path)
    # loop_save_projected_points(input_path)
    # generate_projection_dynamic(input_path, dest_path)


if __name__ == "__main__":
    main()

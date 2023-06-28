"""
Contains a single function to generate a static plot from a parquet file.
"""

import time
import argparse

import datashader as ds
import pandas as pd
import holoviews as hv
import holoviews.operation.datashader as hd
hv.extension('bokeh')
import geoviews as gv
from cartopy import crs
import colorcet as cc

def format_bytes(num_bytes: int):
    """Format the given number of bytes using SI unit suffixes

    https://en.wikipedia.org/wiki/Byte#Multiple-byte_units
    """
    power = 1024
    n = 0
    power_labels = {0: "B", 1: "KiB", 2: "MiB", 3: "GiB", 4: "TiB", 5: "PiB", 6: "EiB"}
    while num_bytes > power:
        num_bytes /= power
        n += 1
    try:
        return f"{num_bytes:,.2f} {power_labels[n]}"
    except IndexError:
        # If we don't have a power label, just return the raw bytes with the appropriate suffix
        return f"{num_bytes:,.2f} B"


def generate_projections_static(input_file: str, dest: str):
    """Reads an input parquet file containing GBT antenna positions and plots a map projection of the positions,
    which is saved to the destination path"""
    # print(f"File size: {format_bytes(os.path.getsize(input_file))}")

    # start = time.perf_counter()
    df = pd.read_parquet(input_file, columns=["DMJD", "RAJ2000", "DECJ2000"])
    # print(f"Loading parquet file: {round(time.perf_counter() - start, 2)}s")

    # print(f"Data frame size: {df.size} rows")
    # print(f"Data frame memory usage: {format_bytes(df.memory_usage(index=True).sum())}")

    # remove "invalid" data
    df = df[(df["RAJ2000"] >= 0) & (df["RAJ2000"] <= 360)]
    df = df[(df["DECJ2000"] >= -90) & (df["DECJ2000"] <= 90)]

    points = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
    points = points.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=2000, height=1000))

    start = time.perf_counter()
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    print(f"Projecting geoviews points: {round(time.perf_counter() - start,2)}s")

    ranges = get_ranges()

    start = time.perf_counter()
    canvas = ds.Canvas(plot_width=2000, plot_height=1000, 
                       x_range=(ranges["RAJ2000"].min(), ranges["RAJ2000"].max()),
                       y_range=(ranges["DECJ2000"].min(), ranges["DECJ2000"].max()))
    canvas_points = canvas.points(projected.data, "RAJ2000", "DECJ2000")
    print(f"Canvas: {round(time.perf_counter() - start, 2)}s")

    start = time.perf_counter()
    ds_image = ds.tf.Image(ds.tf.set_background(ds.tf.shade(canvas_points, cc.rainbow4)))
    print(f"Image: {round(time.perf_counter() - start, 2)}s")

    ds_image.to_pil().save(dest)
    # ds_image.to_pandas().to_parquet(dest)
    print(f"Wrote {dest}")


def get_ranges():
    """Returns a table containing the projected ranges for right ascension and declination"""
    ranges_list = [[0, -90], [360, 90]]
    [ranges_list.append([i, 0]) for i in range(361)]
    ranges_df = pd.DataFrame(ranges_list, columns=["RAJ2000", "DECJ2000"])
    ranges_points = gv.Points(ranges_df, kdims=['RAJ2000', 'DECJ2000'])
    ranges_points = ranges_points.opts(gv.opts.Points(projection=crs.Mollweide()))
    ranges_projected = gv.operation.project_points(ranges_points, projection=crs.Mollweide())
    return ranges_projected.data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="The parquet file used to plot antenna positions")
    parser.add_argument("dest", help="The destination file to save the generated image")
    args = parser.parse_args()
    input_file = str(args.input_file)
    dest = str(args.dest)
    generate_projections_static(input_file, dest)


if __name__ == "__main__":
    main()
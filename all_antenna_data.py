import os
import time
import argparse
import PIL

import datashader as ds
import pandas as pd
import colorcet as cc
import geoviews as gv
from cartopy import crs

gv.extension('bokeh')

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

def generate_projections(input_file: str, dest: str):
    print(f"File size: {format_bytes(os.path.getsize(input_file))}")

    start = time.perf_counter()
    df = pd.read_parquet(input_file, columns=["DMJD", "RAJ2000", "DECJ2000"])
    print(f"Loading parquet file: {round(time.perf_counter() - start, 2)}s")

    print(f"Data frame size: {df.size} rows")
    print(f"Data frame memory usage: {format_bytes(df.memory_usage(index=True).sum())}")

    # remove "invalid" data
    df = df[(df["RAJ2000"] >= 0) & (df["RAJ2000"] <= 360)]
    df = df[(df["DECJ2000"] >= -90) & (df["DECJ2000"] <= 90)]

    start = time.perf_counter()
    points = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
    print(f"Rendering df in geoviews: {round(time.perf_counter() - start, 2)}s")

    points = points.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=2000, height=1000))

    start = time.perf_counter()
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    print(f"Projecting geoviews points: {round(time.perf_counter() - start,2)}s")

    ranges = get_ranges()
    canvas = ds.Canvas(plot_width=2000, plot_height=1000, y_range=(min(ranges["DECJ2000"]), max(ranges["DECJ2000"])))
    canvas_points = canvas.points(projected.data, "RAJ2000", "DECJ2000")

    # ds.tf.Images(ds.tf.set_background(ds.tf.shade(canvas_points, cc.bmw), "black"),
    #             ds.tf.set_background(ds.tf.shade(canvas_points, cc.bgy), "black"),
    #             ds.tf.set_background(ds.tf.shade(canvas_points, cc.CET_L8), "black"))
    
    ds_image = ds.tf.Image(ds.tf.set_background(ds.tf.shade(canvas_points, cc.bmw), "black"))
    ds_image.to_pil().save(dest)


def get_ranges():
    ranges_list = [[0, -90], [180, 90]]
    ranges_df = pd.DataFrame(ranges_list, columns=["RAJ2000", "DECJ2000"])
    ranges_points = gv.Points(ranges_df, kdims=['RAJ2000', 'DECJ2000'])
    ranges_points = ranges_points.opts(gv.opts.Points(projection=crs.Mollweide()))
    ranges_projected = gv.operation.project_points(ranges_points, projection=crs.Mollweide())
    return ranges_projected.data

parser = argparse.ArgumentParser()
parser.add_argument("input_file", help="The parquet file used to plot antenna positions")
parser.add_argument("dest", help="The destination file to save the generated image")
args = parser.parse_args()
input_file = str(args.input_file)
dest = str(args.dest)
generate_projections(input_file, dest)


from pathlib import Path
import time
import argparse
from PIL import Image

import pandas as pd
import datashader as ds
import holoviews as hv
import holoviews.operation.datashader as hd
hv.extension('bokeh')
import geoviews as gv
from cartopy import crs
import colorcet as cc

# xr.concat, tf.shade(merged.sum(dim=''))
# hv.NdOverlay()

def get_ranges():
    """Returns a table containing the projected ranges for right ascension and declination"""
    ranges_list = [[0, -90], [360, 90]]
    [ranges_list.append([i, 0]) for i in range(361)]
    ranges_df = pd.DataFrame(ranges_list, columns=["RAJ2000", "DECJ2000"])
    ranges_points = gv.Points(ranges_df, kdims=['RAJ2000', 'DECJ2000'])
    ranges_points = ranges_points.opts(gv.opts.Points(projection=crs.Mollweide()))
    ranges_projected = gv.operation.project_points(ranges_points, projection=crs.Mollweide())
    return ranges_projected.data


def load_image(path):
    """Reads a datashader Image parquet file and converts it back to a datashader Image"""
    df = pd.read_parquet(path)
    return ds.tf.Image(df)


def stack_images(input_dir, dest_path):
    """Uses ds.tf.stack(*images) to stack datashader Images directly and saves the resultant Image."""
    
    image_path_list = list(Path(input_dir).glob("*.parquet"))

    start = time.perf_counter()
    images = [load_image(path) for path in image_path_list]
    print(f"Loading images: {round(time.perf_counter() - start, 2)}s")

    start = time.perf_counter()
    stacked = ds.tf.stack(*images)
    print(f"Stacking: {round(time.perf_counter() - start, 2)}s")

    stacked.to_pil().save(dest_path)


def stack_images_png(input_dir, dest_path):
    """An attempt to stack png files directly... don't look"""
    
    image_path_list = list(Path(input_dir).glob("*.png"))

    start = time.perf_counter()
    images = [ds.tf.Image(Image.open(path)) for path in image_path_list]
    print(f"Loading images: {round(time.perf_counter() - start, 2)}s")

    start = time.perf_counter()
    stacked = ds.tf.stack(*images)
    print(f"Stacking: {round(time.perf_counter() - start, 2)}s")

    stacked.to_pil().save(dest_path)


def stack_pos_parquets(input_dir, dest_path):
    """Stacks antenna position parquet files and saves the shaded image"""
    image_path_list = list(Path(input_dir).glob("**/*/*.parquet"))
    print(f"Stacking {image_path_list}")
    dfs = [pd.read_parquet(path) for path in image_path_list]
    df = pd.concat(dfs, ignore_index=True)
    
    df = df[(df["RAJ2000"] >= 0) & (df["RAJ2000"] <= 360)]
    df = df[(df["DECJ2000"] >= -90) & (df["DECJ2000"] <= 90)]

    points = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
    points = points.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=2000, height=1000))
    projected = gv.operation.project_points(points, projection=crs.Mollweide())

    ranges = get_ranges()
    canvas = ds.Canvas(plot_width=2000, plot_height=1000, 
                       x_range=(ranges["RAJ2000"].min(), ranges["RAJ2000"].max()),
                       y_range=(ranges["DECJ2000"].min(), ranges["DECJ2000"].max()))
    canvas_points = canvas.points(projected.data, "RAJ2000", "DECJ2000")
    ds_image = ds.tf.Image(ds.tf.set_background(ds.tf.shade(canvas_points, cc.rainbow4)))
    ds_image.to_pil().save(dest_path)


def stack_projected_parquets(input_dir, dest_path):
    """Stacks parquet files of already projected geoviews points data. Either saves a static image
    or a dynamic HTML plot."""
    ranges = get_ranges()
    path_list = list(Path(input_dir).glob("*.parquet"))

    start = time.perf_counter()
    dfs = [pd.read_parquet(path) for path in path_list]
    print(f"Reading parquets: {round(time.perf_counter() - start, 2)}s")

    start = time.perf_counter()
    stacked = pd.concat(dfs, ignore_index=True)
    print(f"Stacking dfs: {round(time.perf_counter() - start, 2)}s")

    # points = gv.Points(stacked, kdims=['RAJ2000', 'DECJ2000'])
    # shaded = hd.datashade(points).opts(projection=crs.Mollweide(), global_extent=True, width=1000, height=500)
    # hv.save(shaded, dest_path)


    canvas = ds.Canvas(plot_width=2000, plot_height=1000, 
                       x_range=(ranges["RAJ2000"].min(), ranges["RAJ2000"].max()),
                       y_range=(ranges["DECJ2000"].min(), ranges["DECJ2000"].max()))

    start = time.perf_counter()
    canvas_points = canvas.points(stacked, "RAJ2000", "DECJ2000")
    print(f"Canvas points: {round(time.perf_counter() - start, 2)}s")

    start = time.perf_counter()
    ds_image = ds.tf.Image(ds.tf.set_background(ds.tf.shade(canvas_points, cc.rainbow4)))
    print(f"Shading image: {round(time.perf_counter() - start, 2)}s")

    ds_image.to_pil().save(dest_path)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=Path, help="The root directory containing plot parquet files")
    parser.add_argument("dest_path", type=Path, help="The destination path for the image")
    args = parser.parse_args()
    return args.input_dir, args.dest_path


def main():
    input_dir, dest_path = parse_arguments()
    # TODO: add arguments for different functions
    # stack_images(input_dir, dest_path)
    # stack_pos_parquets(input_dir, dest_path)
    stack_projected_parquets(input_dir, dest_path)
    # stack_images_png(input_dir, dest_path)


if __name__ == "__main__":
    main()

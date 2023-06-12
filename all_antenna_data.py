# %%
import os
import time

import datashader as ds
import pandas as pd
import colorcet as cc
import geoviews as gv
from cartopy import crs

gv.extension('bokeh')

# %%
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

# %%
# file = "/home/scratch/kwei/antenna_data/ant_pos_300k.parquet"
file = "/home/scratch/tchamber/antenna_data/ant_pos_all_v2.parquet"
print(f"File size: {format_bytes(os.path.getsize(file))}")

start = time.perf_counter()
df = pd.read_parquet(file, columns=["DMJD", "RAJ2000", "DECJ2000"])
print(f"Loading parquet file: {round(time.perf_counter() - start, 2)}s")

print(f"Data frame size: {df.size} rows")
print(f"Data frame memory usage: {format_bytes(df.memory_usage(index=True).sum())}")


# %%
# remove "invalid" data
df = df[(df["RAJ2000"] <= 360) & (df["RAJ2000"] >= 0)]
df = df[(df["DECJ2000"] <= 90) & (df["DECJ2000"] >= -90)]
df["RAJ2000"] = df["RAJ2000"] - 180

# %%
start = time.perf_counter()
points = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
print(f"Rendering df in geoviews: {round(time.perf_counter() - start, 2)}s")

points = points.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=2000, height=1000))

start = time.perf_counter()
projected = gv.operation.project_points(points, projection=crs.Mollweide())
print(f"Projecting geoviews points: {round(time.perf_counter() - start,2)}s")

# %%
canvas = ds.Canvas(plot_width=2000, plot_height=1000)
canvas_points = canvas.points(projected.data, "RAJ2000", "DECJ2000")

# %%
shaded = ds.transfer_functions.shade(canvas_points, cmap=cc.bgy)
shaded

# %%
ds.tf.Images(ds.tf.set_background(ds.tf.shade(canvas_points, cc.bmw), "black"),
             ds.tf.set_background(ds.tf.shade(canvas_points, cc.bgy), "black"),
             ds.tf.set_background(ds.tf.shade(canvas_points, cc.CET_L8), "black"))



# %%
import os
from pathlib import Path

import datashader as ds, pandas as pd, colorcet as cc
from datashader.utils import export_image
from astropy.table import Table, vstack
import astropy.coordinates as coord
import astropy.units as u

# %%
def stack_tables(directory: str):
    """Returns combined tables of RA and DEC data for all files in a directory"""
    stacked_tables = []
    for filename in Path(directory).glob("*.fits"):
        full_table = Table.read(filename, format='fits', hdu=2)
        sliced_table = full_table[["RAJ2000", "DECJ2000"]]
        stacked_tables.append(sliced_table)
    return vstack(stacked_tables)

# %%
root = "/home/scratch/kwei/raw_data/AGBT13B_312_34/Antenna"
table = stack_tables(root)
ra = coord.Angle(table["RAJ2000"])
table["RAJ2000"] = ra.wrap_at(180 * u.degree)
table["DECJ2000"] = coord.Angle(table["DECJ2000"])
df = table.to_pandas()
df

# %%
agg = ds.Canvas().points(df, 'RAJ2000', 'DECJ2000')
ant_data = ds.tf.set_background(ds.tf.shade(agg, cmap=cc.rainbow), "white")
ant_data
# export_image(ds.tf.shade(agg, cmap=cc.rainbow))

# %%
import holoviews as hv
from holoviews.operation.datashader import datashade
hv.extension('bokeh')

# %%
points = hv.Points(df, ['RAJ2000', 'DECJ2000'])
#.redim.label(x='RA J2000 (deg)', y='DEC J2000 (deg)')
# points = points.options(xlabel='RA J2000 (deg)', ylabel='DEC J2000 (deg)')
antenna_plot = datashade(points, cmap=cc.rainbow)
antenna_plot

# %%
import geoviews as gv
from cartopy import crs

gv.extension('bokeh')

# %%
points2 = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
gv.output(points2)

# %%
points2 = points2.relabel("AGBT13B_312_34")
points2 = points2.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=1000, height=500))
plot = gv.operation.project_points(points2, projection=crs.Mollweide())
gv.output(plot * gv.feature.grid())

# %%
file = "/home/scratch/tchamber/antenna_data/ant_pos_all_v2.parquet"
full_df = pd.read_parquet(file, columns=["DMJD", "RAJ2000", "DECJ2000"])
full_df

# %%
# remove outliers - histogram
full_df = full_df[(full_df["RAJ2000"] <= 180) & (full_df["RAJ2000"] >= -180)]
full_df = full_df[(full_df["DECJ2000"] <= 90) & (full_df["DECJ2000"] >= -90)]
full_df
full_df.agg([min, max])


# %%
full_agg = ds.Canvas().points(full_df, x='RAJ2000', y='DECJ2000')
full_data = ds.tf.set_background(ds.tf.shade(full_agg), "white")
full_data

# %%
full_points = hv.Points(full_df, kdims=['RAJ2000', 'DECJ2000'])
full_antenna_plot = datashade(full_points, x_range=(-180, 180), y_range=(-90,90))
full_antenna_plot

# %%
# colormaps = [cc.bmw, cc.kg, cc.CET_L17]
ds.tf.Images(datashade(full_points, x_range=(0, 360), y_range=(0,360), cmap=cc.bmw, name="bmw"),
          datashade(full_points, x_range=(0, 360), y_range=(0,360), cmap=cc.kg, name="kg"), 
          datashade(full_points, x_range=(0, 360), y_range=(0,360), cmap=cc.CET_L17, name="CET_L17"))

# %%
full_points2 = gv.Points(full_df, kdims=['RAJ2000', 'DECJ2000'])
full_points2 = full_points2.relabel("Antenna data")
full_points2 = full_points2.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=1000, height=500))
plot2 = gv.operation.project_points(full_points2, projection=crs.Mollweide())
gv.output(plot2 * gv.feature.grid())

# %%
smaller = full_df[::10000]




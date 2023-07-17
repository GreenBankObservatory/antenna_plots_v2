import pandas as pd

import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd
import geoviews as gv
from cartopy import crs
from colorcet import cm
import param

hv.extension('bokeh', logo=False)


def remove_invalid_data(df: pd.DataFrame):
    """Removes data lying outside (0, 360) degrees RA and (-90, 90) degrees declination"""
    df = df[(df["RAJ2000"] >= 0) & (df["RAJ2000"] <= 360)]
    df = df[(df["DECJ2000"] >= -90) & (df["DECJ2000"] <= 90)]
    return df


def get_ranges():
    """Returns a table containing the projected ranges for right ascension and declination"""
    ranges_list = [[0, -90], [360, 90]]
    [ranges_list.append([i, 0]) for i in range(361)]
    ranges_df = pd.DataFrame(ranges_list, columns=["RAJ2000", "DECJ2000"])
    ranges_points = gv.Points(ranges_df, kdims=['RAJ2000', 'DECJ2000'])
    ranges_points = ranges_points.opts(gv.opts.Points(projection=crs.Mollweide()))
    ranges_projected = gv.operation.project_points(ranges_points, projection=crs.Mollweide())
    return ranges_projected.data


def project(df: pd.DataFrame):
    """Returns projected geoviews points from an input dataframe of antenna positions"""
    points = gv.Points(df, kdims=['RAJ2000', 'DECJ2000'])
    points = points.opts(gv.opts.Points(projection=crs.Mollweide(), global_extent=True, width=2000, height=1000))
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    return projected


df = pd.read_parquet("more_sessions.parquet")
df = remove_invalid_data(df)
cmaps = ['rainbow4','bgy','bgyw','bmy','gray','kbc']
sessions = df["Session"].unique().tolist()
sessions.append("ALL")

class AntennaPositionExplorer(param.Parameterized):
    cmap = param.ObjectSelector(cm['rainbow4'], objects={c:cm[c] for c in cmaps})
    session = param.ObjectSelector('ALL', objects=sessions)
    # RA/DEC?

    def view(self,**kwargs):
        dataframe = df
        # if self.param.session != 'ALL':
        #     dataframe = df[df['Session'] == self.param.session]
        projected = project(dataframe)
        shaded = hd.datashade(projected, cmap=self.param.cmap).opts(projection=crs.Mollweide(), global_extent=True, width=800, height=400)
        return shaded

ant_pos = AntennaPositionExplorer(name="Test Interactive Dashboard")
pn.Row(ant_pos.param, ant_pos.view()).servable()
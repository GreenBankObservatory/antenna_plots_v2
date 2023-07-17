import datetime as dt
import pandas as pd

import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd
import geoviews as gv
from cartopy import crs
from colorcet import cm
import param

hv.extension("bokeh", logo=False)


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
    ranges_points = gv.Points(ranges_df, kdims=["RAJ2000", "DECJ2000"])
    ranges_points = ranges_points.opts(gv.opts.Points(projection=crs.Mollweide()))
    ranges_projected = gv.operation.project_points(
        ranges_points, projection=crs.Mollweide()
    )
    return ranges_projected.data


def project(df: pd.DataFrame):
    """Returns projected geoviews points from an input dataframe of antenna positions"""
    points = gv.Points(df, kdims=["RAJ2000", "DECJ2000"], vdims=["Session"])
    points = points.opts(
        gv.opts.Points(
            projection=crs.Mollweide(), global_extent=True, width=2000, height=1000
        )
    )
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    return projected


dataset = pd.read_parquet("more_sessions.parquet")
dataset = remove_invalid_data(dataset)
cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]
sessions = [""] + dataset["Session"].unique().tolist()
# frontends = [""] + dataset["Frontend"].unique().tolist()
# backends = [""] + dataset["Backend"].unique().tolist()
# proc_names = [""] + dataset["ProcName"].unique().tolist()


class AntennaPositionExplorer(param.Parameterized):
    cmap = param.ObjectSelector(cm["rainbow4"], objects={c: cm[c] for c in cmaps})
    session = param.String("")
    # observer = param.String('')
    # frontend = param.ObjectSelector('', objects=frontends)
    # backend = param.ObjectSelector('', objects=backends)
    # scan_number = param.Range(default=(0,1000), bounds=(0,1000))
    # scan_start = param.Range()
    # proc_name = param.String('')

    # RA/DEC?

    def get_data(self):
        df = dataset[
            (dataset["Session"].str.contains(self.session))
            # & (dataset["Observer"].str.contains(self.observer))
            # & (dataset["Frontend"].isin(self.frontend))
            # & (dataset["Backend"].isin(self.backend))
            # & (dataset["ScanNumber"].isin(range(self.scan_number)))
            # & (dataset["ScanStart"].isin(range(self.scan_start)))
            # & (dataset["ProcName"].str.contains(self.proc_name))
        ].copy()
        return df

    def view(self, **kwargs):
        df = self.get_data()
        projected = project(df)
        shaded = hd.datashade(projected, cmap=self.param.cmap).opts(
            projection=crs.Mollweide(), global_extent=True, width=800, height=400
        )
        return shaded


ant_pos = AntennaPositionExplorer(name="GBT Antenna Interactive Dashboard")
widgets = pn.Param(
    ant_pos.param,
    widgets={
        "session": {
            "type": pn.widgets.AutocompleteInput(
                options=sessions, restrict=False, name="Sessions"
            )
        },
        # "observer": {
        #     "type": pn.widgets.AutocompleteInput(
        #         options=observers, restrict=False, name="Observer"
        #     )
        # },
        # "frontend": {
        #     "type": pn.widgets.MultiSelect(options=frontends, name="Frontends")
        # },
        # "backend": {"type": pn.widgets.MultiSelect(options=backends, name="Backends")},
        # "scan_number": {
        #     "type": pn.widgets.IntRangeSlider(start=0, end=1000, name="Number of scans")
        # },
        # "scan_start": {
        #     "type": pn.widgets.DatetimeRangeInput(
        #         start=dt.datetime(2002, 1, 1),
        #         end=dt.datetime.today(),
        #         name="Number of scans",
        #     )
        # },
        # "proc_name": {
        #     "type": pn.widgets.AutocompleteInput(
        #         options=proc_names, restrict=False, name="Proc names?"
        #     )
        # },
    },
)
pn.Row(widgets, ant_pos.view).servable()

# to run:
# panel serve --show ant_pos_panel_server.py

import datetime as dt

import numpy as np
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


def project(df: pd.DataFrame):
    """Returns projected geoviews points from an input dataframe of antenna positions"""
    points = gv.Points(df, kdims=["RAJ2000", "DECJ2000"])
    points = points.opts(
        gv.opts.Points(
            projection=crs.Mollweide(), global_extent=True, width=2000, height=1000
        )
    )
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    return projected


dataset = pd.read_parquet("fewer_sessions.parquet")
dataset = remove_invalid_data(dataset)

cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]
sessions = [""] + dataset["Session"].unique().tolist()
# frontends = dataset["Frontend"].unique().tolist()
# backends = dataset["Backend"].unique().tolist()
# proc_names = [""] + dataset["ProcName"].unique().tolist()

# FAKE data
df_size = len(dataset)
observers = ["", "Will Armentrout", "Emily Moravec", "Thomas Chamberlin", "Cat Catlett"]
frontends = ["grote", "reber", "karl", "jansky"]
backends = ["a", "aa", "aaa", "aaaa", "aaaaa"]
proc_names = ["", "i'm", "a", "little", "teacup"]
dataset["Observer"] = np.random.choice(observers, size=df_size)
dataset["Frontend"] = np.random.choice(frontends, size=df_size)
dataset["Backend"] = np.random.choice(backends, size=df_size)
dataset["ProcName"] = np.random.choice(proc_names, size=df_size)
dataset["ScanNumber"] = np.random.randint(low=0, high=1000, size=df_size)
dataset["ScanStart"] = [dt.datetime(2007, 4, 24)] * int(df_size / 2) + [
    dt.datetime(2023, 7, 1)
] * (df_size - int((len(dataset) / 2)))


class AntennaPositionExplorer(param.Parameterized):
    cmap = param.Selector(default=cm["rainbow4"], objects={c: cm[c] for c in cmaps})
    session = param.String("")
    observer = param.String("")
    frontend = param.ListSelector(default=frontends, objects=frontends)
    backend = param.ListSelector(default=backends, objects=backends)
    scan_number = param.Range(default=(0, 1000), bounds=(0, 1000))
    scan_start = param.DateRange(
        default=(dt.datetime(2002, 1, 1), dt.datetime(2023, 7, 17)),
        bounds=(dt.datetime(2002, 1, 1), dt.datetime.today()),
    )
    proc_name = param.String("")

    # RA/DEC?

    @param.depends(
        "session",
        "observer",
        "frontend",
        "backend",
        "scan_number",
        "scan_start",
        "proc_name",
    )    
    def get_data(self):
        df = dataset[
            (dataset["Session"].str.contains(self.session))
            & (dataset["Observer"].str.contains(self.observer))
            & (dataset["Frontend"].isin(self.frontend))
            & (dataset["Backend"].isin(self.backend))
            & (dataset["ScanNumber"] >= self.scan_number[0])
            & (dataset["ScanNumber"] <= self.scan_number[1])
            & (dataset["ScanStart"] >= self.scan_start[0])
            & (dataset["ScanStart"] <= self.scan_start[1])
            & (dataset["ProcName"].str.contains(self.proc_name))
        ].copy()
        print(df)
        return df

    def view(self, **kwargs):
        df = self.get_data()
        projected = project(df)
        shaded = hd.datashade(projected, cmap=self.param.cmap).opts(
            projection=crs.Mollweide(), global_extent=True, width=800, height=400
        )
        print("hi")
        return shaded * gv.feature.grid()


ant_pos = AntennaPositionExplorer(name="GBT Antenna Interactive Dashboard")
widgets = pn.Param(
    ant_pos.param,
    widgets={
        "session": {
            "type": pn.widgets.AutocompleteInput(
                options=sessions, restrict=False, name="Sessions"
            )
        },
        "observer": {
            "type": pn.widgets.AutocompleteInput(
                options=observers, restrict=False, name="Observer"
            )
        },
        "scan_start": {
            "type": pn.widgets.DatetimeRangeInput(
                start=dt.datetime(2002, 1, 1),
                end=dt.datetime.today(),
                value=(dt.datetime(2002, 1, 1), dt.datetime(2023, 7, 17)),
                name="Date and time of scan",
            )
        },
        "proc_name": {
            "type": pn.widgets.AutocompleteInput(
                options=proc_names, restrict=False, min_characters=0, name="Proc names?"
            )
        },
    },
)
pn.Row(widgets, ant_pos.view).servable()

# to run:
# panel serve --show ant_pos_panel_server.py

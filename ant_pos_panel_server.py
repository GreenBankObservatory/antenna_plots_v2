from datetime import datetime, timedelta
import time

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


def project(points):
    """Returns projected geoviews points from an input dataframe of antenna positions"""
    points = points.opts(
        gv.opts.Points(
            projection=crs.Mollweide(), global_extent=True, width=2000, height=1000
        )
    )
    projected = gv.operation.project_points(points, projection=crs.Mollweide())
    return projected


print("Reading the parquet file into memory...")
start = time.perf_counter()
dataset = pd.read_parquet("more_sessions.parquet")
print(f"Elapsed time: {time.perf_counter() - start}s")

dataset = remove_invalid_data(dataset)

cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]
sessions = [""] + dataset["Session"].unique().tolist()
# frontends = dataset["Frontend"].unique().tolist()
# backends = dataset["Backend"].unique().tolist()
# proc_names = [""] + dataset["ProcName"].unique().tolist()

# FAKE data
df_size = len(dataset)
print("Generating fake data...")
start = time.perf_counter()
observers = ["", "Will Armentrout", "Emily Moravec", "Thomas Chamberlin", "Cat Catlett"]
frontends = ["grote", "reber", "karl", "jansky"]
backends = ["i'm", "a", "little", "teacup"]
proc_names = ["", "a", "aa", "aaa", "aaaa", "aaaaa"]
dataset["Observer"] = np.random.choice(observers, size=df_size)
dataset["Frontend"] = np.random.choice(frontends, size=df_size)
dataset["Backend"] = np.random.choice(backends, size=df_size)
dataset["ProcName"] = np.random.choice(proc_names, size=df_size)
dataset["ScanNumber"] = np.random.randint(low=0, high=1000, size=df_size)
dataset["ScanStart"] = [datetime(2007, 4, 24)] * int(df_size / 2) + [
    datetime(2023, 7, 1)
] * (df_size - int((len(dataset) / 2)))
print(f"Elapsed time: {time.perf_counter() - start}s")


class AntennaPositionExplorer(param.Parameterized):
    cmap = param.Selector(default=cm["rainbow4"], objects={c: cm[c] for c in cmaps})
    session = param.String("")
    observer = param.String("")
    frontend = param.ListSelector(default=frontends, objects=frontends)
    backend = param.ListSelector(default=backends, objects=backends)
    scan_number = param.Range(default=(0, 1000), bounds=(0, 1000))
    scan_start = param.DateRange(
        default=(datetime(2002, 1, 1), datetime(2023, 7, 17)),
        bounds=(datetime(2002, 1, 1), datetime.today()),
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
    def points(self):
        print("Generating geoviews points...")
        start = time.perf_counter()
        points = gv.Points(
            dataset,
            kdims=["RAJ2000", "DECJ2000"],
            vdims=[
                "Session",
                "Observer",
                "Frontend",
                "Backend",
                "ProcName",
                "ScanNumber",
                "ScanStart",
            ],
        )
        print(f"Elapsed time: {time.perf_counter() - start}s")

        print("Selecting data...")
        start = time.perf_counter()
        sessions_list = list(
            filter(lambda session: self.session in session, sessions)
        )
        points = points.select(
            Session=sessions_list,
            Frontend=self.frontend,
            Backend=self.backend,
            ScanNumber=self.scan_number,
            ScanStart=self.scan_start,
        )
        if self.observer:
            points = points.select(Observer=self.observer)
        if self.proc_name:
            points = points.select(ProcName=self.proc_name)
        print(f"Elapsed time: {time.perf_counter() - start}s")

        return project(points)

    def view(self, **kwargs):
        points = hv.DynamicMap(self.points)
        print("Rasterizing and shading plot...")
        start = time.perf_counter()
        agg = hd.rasterize(
            points,
            # x_sampling=1,
            # y_sampling=1,
        )
        plot = (
            hd.shade(agg, cmap=self.param.cmap).opts(
                projection=crs.Mollweide(),
                global_extent=True,
                width=800,
                height=400,
            )
            * gv.feature.grid()
        )
        print(f"Elapsed time: {time.perf_counter() - start}s")
        return plot


ant_pos = AntennaPositionExplorer(name="GBT Antenna Interactive Dashboard")
print("Generating widgets...")
start = time.perf_counter()
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
            "type": pn.widgets.DatetimeRangePicker(
                start=datetime(2002, 1, 1),
                end=datetime.today(),
                value=(datetime(2002, 1, 1), datetime.today() - timedelta(days=1)),
                name="Date and time of first scan",
            )
        },
        "proc_name": {
            "type": pn.widgets.AutocompleteInput(
                options=proc_names, restrict=False, min_characters=1, name="Proc names?"
            )
        },
    },
)
print(f"Elapsed time: {time.perf_counter() - start}s")
pn.Row(widgets, ant_pos.view()).servable()

# to run:
# panel serve --show ant_pos_panel_server.py

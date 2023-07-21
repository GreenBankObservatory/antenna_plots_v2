from datetime import datetime, timedelta
import time

import pandas as pd

import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd
import geoviews as gv
from cartopy import crs
from colorcet import cm
import param

hv.extension("bokeh", logo=False)
hv.config(future_deprecations=False)


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
dataset = pd.read_parquet("/home/scratch/kwei/misc_parquets/multiindex_projection.parquet")
print(f"Elapsed time: {time.perf_counter() - start}s")

# all_points = gv.Points(
#     dataset,
#     kdims=["RAJ2000", "DECJ2000"],
#     vdims=[
#         "Session",
#         "Observer",
#         "Frontend",
#         "Backend",
#         "ProcName",
#         "ScanNumber",
#         "ScanStart",
#     ],
# )
# projected = project(all_points)

cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]
# sessions = dataset["Session"].unique().tolist()
sessions = [""] + dataset.index.get_level_values(1).unique().tolist()
# frontends = dataset["Frontend"].unique().tolist()
# backends = dataset["Backend"].unique().tolist()
# proc_names = [""] + dataset["ProcName"].unique().tolist()

# FAKE data
observers = ["", "Will Armentrout", "Emily Moravec", "Thomas Chamberlin", "Cat Catlett"]
frontends = ["grote", "reber", "karl", "jansky"]
backends = ["i'm", "a", "little", "teacup"]
proc_names = ["", "a", "aa", "aaa", "aaaa", "aaaaa"]

cur_datetime = datetime.today()


class AntennaPositionExplorer(param.Parameterized):
    cmap = param.Selector(default=cm["rainbow4"], objects={c: cm[c] for c in cmaps})
    session = param.String("")
    observer = param.String("")
    frontend = param.ListSelector(default=frontends, objects=frontends)
    backend = param.ListSelector(default=backends, objects=backends)
    scan_number = param.Range(default=(0, 1000), bounds=(0, 1000))
    scan_start = param.DateRange(
        default=(datetime(2002, 1, 1), cur_datetime - timedelta(days=1)),
        bounds=(datetime(2002, 1, 1), cur_datetime),
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
        print("Selecting data...")
        start = time.perf_counter()
        # breakpoint()
        filtered = dataset
        if self.session:
            checkpoint = time.perf_counter()
            filtered = filtered.xs(self.session, level=0, drop_level=False)
            print(f"Filter by session: {time.perf_counter() - checkpoint}s")
        if self.observer:
            checkpoint = time.perf_counter()
            filtered = filtered.loc[pd.IndexSlice[:, self.observer], :]
            print(f"Filter by observer: {time.perf_counter() - checkpoint}s")
        if self.proc_name:
            checkpoint = time.perf_counter()
            filtered = filtered.loc[pd.IndexSlice[:, :, :, :, self.proc_name], :]
            print(f"Filter by proc_name: {time.perf_counter() - checkpoint}s")
        if self.scan_number != (0, 1000):
            checkpoint = time.perf_counter()
            scan_numbers = filtered.index.get_level_values("ScanNumber")
            filtered = filtered[(scan_numbers>= self.scan_number[0]) & (scan_numbers < self.scan_number[1])]
            print(f"Filter by scan_number: {time.perf_counter() - checkpoint}s")
        if self.scan_start != (datetime(2002, 1, 1), cur_datetime - timedelta(days=1)):
            checkpoint = time.perf_counter()
            scan_starts = filtered.index.get_level_values("ScanStart")
            filtered = filtered[(scan_starts >= self.scan_start[0]) & (scan_starts < self.scan_start[1])]
            print(f"Filter by scan_start: {time.perf_counter() - checkpoint}s")
        if self.frontend != frontends:
            checkpoint = time.perf_counter()
            cur_frontends = filtered.index.get_level_values("Frontend")
            filtered = filtered[cur_frontends.isin(self.frontend)]
            print(f"Filter by frontend: {time.perf_counter() - checkpoint}s")
        if self.backend != backends:
            checkpoint = time.perf_counter()
            cur_backends = filtered.index.get_level_values("Backend")
            filtered = filtered[cur_backends.isin(self.backend)]
            print(f"Filter by backend: {time.perf_counter() - checkpoint}s")
        # checkpoint = time.perf_counter()
        # filtered = filtered.loc[
        #     pd.IndexSlice[
        #         :,
        #         :,
        #         self.frontend,
        #         self.backend,
        #         # self.scan_number,
        #         # self.scan_start,
        #         # self.proc_name,
        #     ],
        #     :,
        # ]
        # print(f"Filter by front/backend: {time.perf_counter() - checkpoint}s")
        print(f"Elapsed time: {time.perf_counter() - start}s")
        points = gv.Points(filtered, kdims=["x_position", "y_position"], crs=crs.Mollweide())
        points = points.opts(
            gv.opts.Points(
                projection=crs.Mollweide(), global_extent=True, width=800, height=400
            )
        )
        return points

    def view(self, **kwargs):
        points = hv.DynamicMap(self.points)
        print("Rasterizing and shading plot...")
        start = time.perf_counter()
        agg = hd.rasterize(
            points,
            x_sampling=1,
            y_sampling=1,
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
                value="", options=observers, name="Observer"
            )
        },
        "scan_start": {
            "type": pn.widgets.DatetimeRangePicker(
                start=datetime(2002, 1, 1),
                end=cur_datetime,
                value=(datetime(2002, 1, 1), cur_datetime - timedelta(days=1)),
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

template = pn.template.BootstrapTemplate(
    title="GBT Antenna Data Interactive Dashboard",
    sidebar=widgets,
    theme=pn.template.DarkTheme,
)
template.main.append(pn.Row(ant_pos.view()))
template.servable()

# to run:
# panel serve --show ant_pos_panel_server.py

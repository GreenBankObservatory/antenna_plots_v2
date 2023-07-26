print("Importing...")
from datetime import datetime, timedelta
import time
import argparse

import pandas as pd

import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd
import geoviews as gv
from cartopy import crs
from colorcet import cm
import param

hv.extension("bokeh", logo=False)
# pn.extension(loading_spinner='dots', reuse_sessions='True', throttled='True')


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_file")
    args = parser.parse_args()
    return args.parquet_file


print("Reading the parquet file into memory...")
start = time.perf_counter()
parquet_file = parse_arguments()
dataset = pd.read_parquet(parquet_file)
print(f"Elapsed time: {time.perf_counter() - start}s")


cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]
sessions = dataset.index.get_level_values("Session").unique().tolist()
observers = dataset.index.get_level_values("Observer").unique().tolist()
frontends = dataset.index.get_level_values("Frontend").unique().tolist()
backends = dataset.index.get_level_values("Backend").unique().tolist()
proc_names = dataset.index.get_level_values("ProcName").unique().tolist()
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
        filtered = dataset
        if self.session:
            checkpoint = time.perf_counter()
            if self.session in sessions:
                filtered = filtered.xs(self.session, level=0, drop_level=False)
            else:
                cur_sessions = filtered.index.get_level_values("Session")
                filtered_sessions = [
                    session for session in sessions if self.session in session
                ]
                filtered = filtered[cur_sessions.isin(filtered_sessions)]
            print(f"Filter by session: {time.perf_counter() - checkpoint}s")
        if self.observer:
            checkpoint = time.perf_counter()
            if self.observer in observers:
                filtered = filtered.loc[pd.IndexSlice[:, self.observer], :]
            else:
                cur_observers = filtered.index.get_level_values("Observer")
                filtered_observers = [
                    observer for observer in observers if self.observer in observer
                ]
                filtered = filtered[cur_observers.isin(filtered_observers)]
            print(f"Filter by observer: {time.perf_counter() - checkpoint}s")
        if self.proc_name:
            checkpoint = time.perf_counter()
            if self.proc_name in proc_names:
                filtered = filtered.loc[
                    pd.IndexSlice[:, :, :, :, :, :, self.proc_name], :
                ]
            else:
                cur_proc_names = filtered.index.get_level_values("ProcName")
                filtered_proc_names = [
                    proc_name for proc_name in proc_names if self.proc_name in proc_name
                ]
                filtered = filtered[cur_proc_names.isin(filtered_proc_names)]
            print(f"Filter by proc_name: {time.perf_counter() - checkpoint}s")
        if self.scan_number != (0, 1000):
            checkpoint = time.perf_counter()
            scan_numbers = filtered.index.get_level_values("ScanNumber")
            filtered = filtered[
                (scan_numbers >= self.scan_number[0])
                & (scan_numbers < self.scan_number[1])
            ]
            print(f"Filter by scan_number: {time.perf_counter() - checkpoint}s")
        if self.scan_start != (datetime(2002, 1, 1), cur_datetime - timedelta(days=1)):
            checkpoint = time.perf_counter()
            scan_starts = filtered.index.get_level_values("ScanStart")
            filtered = filtered[
                (scan_starts >= self.scan_start[0]) & (scan_starts < self.scan_start[1])
            ]
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
        print(f"Elapsed time: {time.perf_counter() - start}s")
        points = gv.Points(
            filtered, kdims=["x_position", "y_position"], crs=crs.Mollweide()
        )
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
                width=900,
                height=450,
            )
            * gv.feature.grid()
        )
        print(f"Elapsed time: {time.perf_counter() - start}s")
        return plot


ant_pos = AntennaPositionExplorer()
print("Generating widgets...")
start = time.perf_counter()
widgets = pn.Param(
    ant_pos.param,
    widgets={
        "session": {
            "type": pn.widgets.AutocompleteInput(
                options=sessions,
                value="",
                restrict=False,
                min_characters=1,
                name="Sessions",
            )
        },
        "observer": {
            "type": pn.widgets.AutocompleteInput(
                options=observers,
                value="",
                restrict=False,
                min_characters=1,
                name="Observers",
            )
        },
        "scan_start": {
            "type": pn.widgets.DatetimeRangeInput(
                start=datetime(2002, 1, 1),
                end=cur_datetime,
                value=(datetime(2002, 1, 1), cur_datetime - timedelta(days=1)),
                name="Date and time of first scan",
            )
        },
        "proc_name": {
            "type": pn.widgets.AutocompleteInput(
                options=proc_names,
                value="",
                restrict=False,
                min_characters=1,
                name="Proc names",
            )
        },
    },
)
print(f"Elapsed time: {time.perf_counter() - start}s")

template = pn.template.BootstrapTemplate(
    title="GBT Antenna Data Interactive Dashboard",
    sidebar=widgets,
    header_background="RoyalBlue"
)
template.main.append(pn.Row(ant_pos.view()))
template.servable()

# to run:
# panel serve ant_pos_panel_server.py --allow-websocket-origin [address] --args [parquet file]

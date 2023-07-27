print("Importing...")
from datetime import datetime, timedelta
import time
import argparse

import pandas as pd

from bokeh.models import HoverTool
import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd
from holoviews import streams
import geoviews as gv
from cartopy import crs
from colorcet import cm
import param

hv.extension("bokeh", logo=False)
# pn.extension(loading_spinner='dots', reuse_sessions='True', throttled='True')

LINK_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-up-right-square" viewBox="0 0 16 16">
  <path fill-rule="evenodd" d="M15 2a1 1 0 0 0-1-1H2a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V2zM0 2a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V2zm5.854 8.803a.5.5 0 1 1-.708-.707L9.243 6H6.475a.5.5 0 1 1 0-1h3.975a.5.5 0 0 1 .5.5v3.975a.5.5 0 1 1-1 0V6.707l-4.096 4.096z"/>
</svg>
"""


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet_file")
    parser.add_argument("alda_address")
    args = parser.parse_args()
    return args.parquet_file, args.alda_address


def get_data(parquet_file):
    print("Reading the parquet file into memory...")
    start = time.perf_counter()
    dataset = pd.read_parquet(parquet_file)
    print(f"Elapsed time: {time.perf_counter() - start}s")
    return dataset


parquet_file, alda_address = parse_arguments()
dataset = get_data(parquet_file)
cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]
sessions = dataset.index.get_level_values("Session").unique().tolist()
observers = dataset.index.get_level_values("Observer").unique().tolist()
frontends = dataset.index.get_level_values("Frontend").unique().tolist()
backends = dataset.index.get_level_values("Backend").unique().tolist()
proc_names = dataset.index.get_level_values("ProcName").unique().tolist()
cur_datetime = datetime.today()


tabulator = pn.widgets.Tabulator(
    value=pd.DataFrame(),  # TODO: populate with empty columns, add formatters
    selectable="True",
    disabled=True,
    show_index=False,
    name="Selected data",
)


def update_tabulator(bounds):
    filtered = dataset
    filtered = filtered[
        (filtered["x_position"] >= bounds[0])
        & (filtered["x_position"] < bounds[2])
        & (filtered["y_position"] >= bounds[1])
        & (filtered["y_position"] < bounds[3])
    ]
    df = pd.DataFrame(
        data={"Session": filtered.index.get_level_values("Session").unique().tolist()}
    )
    # df["Archive"] = df.apply(
    #     lambda row: f"""<a href='http://{alda_address}/disk/sessions/{row.Session}/' target='_blank'>
    #         <div title='View in archive'>{LINK_SVG}</div></a>""",
    #     axis=1,
    # )
    df["Archive"] = [f"""<a href='http://{alda_address}/disk/sessions/{session}/' target='_blank'>
             <div title='View in archive'>{LINK_SVG}</div></a>""" for session in df["Session"]]
    tabulator.value = df
    tabulator.formatters = {"Archive": {"type": "html", "field": "html"}}


class AntennaPositionExplorer(param.Parameterized):
    # TODO: replace with reactive API? move tabulator under plot
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
            filtered,
            kdims=["x_position", "y_position"],
            vdims=["RAJ2000", "DECJ2000"],
            crs=crs.Mollweide(),
        )
        points = points.opts(
            gv.opts.Points(
                projection=crs.Mollweide(), global_extent=True, width=800, height=400
            )
        )
        return points

    def view(self, **kwargs):
        points = hv.DynamicMap(self.points)
        agg = hd.rasterize(
            points,
            x_sampling=1,
            y_sampling=1,
        )
        shaded = hd.shade(agg, cmap=self.param.cmap).opts(
            projection=crs.Mollweide(),
            global_extent=True,
            width=900,
            height=450,
        )

        box = streams.BoundsXY(source=shaded, bounds=(0, 0, 0, 0))
        box.add_subscriber(update_tabulator)
        bounds = hv.DynamicMap(lambda bounds: hv.Bounds(bounds).opts(color='royalblue'), streams=[box])

        plot = shaded.opts(tools=["box_select"]) * gv.feature.grid() * bounds

        return plot


ant_pos = AntennaPositionExplorer(name="")
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


def filter_session(event):
    session = tabulator.value["Session"].iloc[event.row]
    prev_session = ant_pos.session
    if session == prev_session:
        ant_pos.session = ""
    else:
        ant_pos.session = session

tabulator.on_click(filter_session)


template = pn.template.BootstrapTemplate(
    title="GBT Antenna Data Interactive Dashboard",
    sidebar=widgets,
    header_background="RoyalBlue",
)
template.main.append(pn.Column(ant_pos.view(), tabulator))
template.servable()
# TODO: set name of webpage

# to run:
# panel serve ant_pos_panel_server.py --allow-websocket-origin [address] --args [parquet file] [alda address]

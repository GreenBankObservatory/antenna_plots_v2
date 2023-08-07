print("Importing...")
from datetime import datetime, timedelta
import time
import argparse

import pandas as pd

from bokeh.models import BoxSelectTool, BoxAnnotation
import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd
from holoviews import streams
import geoviews as gv
from cartopy import crs
from colorcet import cm
import param

hv.extension("bokeh", logo=False)
pn.extension(
    "tabulator", loading_spinner="dots", reuse_sessions="True", throttled="True"
)

LINK_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-up-right-square" viewBox="0 0 16 16">
  <path fill-rule="evenodd" d="M15 2a1 1 0 0 0-1-1H2a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V2zM0 2a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V2zm5.854 8.803a.5.5 0 1 1-.708-.707L9.243 6H6.475a.5.5 0 1 1 0-1h3.975a.5.5 0 0 1 .5.5v3.975a.5.5 0 1 1-1 0V6.707l-4.096 4.096z"/>
</svg>
"""


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("full_data_path")
    parser.add_argument("metadata_path")
    parser.add_argument("alda_address")
    args = parser.parse_args()
    return args.full_data_path, args.metadata_path, args.alda_address


@pn.cache
def get_data(full_data_path, metadata_path):
    print("Reading parquet files into memory...")
    start = time.perf_counter()
    dataset = pd.read_parquet(full_data_path)
    metadata = pd.read_parquet(metadata_path)
    print(f"Elapsed time: {time.perf_counter() - start}s")
    return dataset, metadata


# TODO: find out ways to prevent insane load time in beginning?
# TODO: precompute archive link
# TODO: have tabulator update with filters, change tabulator so that it only includes filtered results?
# TODO: add 'help' or descriptions for filters
# TODO: resize table?, delete box, fix bug when deselecting tabulator row
# TODO: code better

full_data_path, metadata_path, alda_address = parse_arguments()
dataset, metadata = get_data(full_data_path, metadata_path)
current_filtered = dataset
cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]
param_dict = {}
params = [
    "project",
    "session",
    "observer",
    "frontend",
    "backend",
    "procname",
    "obstype",
    "procscan",
    "proctype",
    "object",
    "script_name",
]
for p in params:
    param_list = dataset.index.get_level_values(p).unique().tolist()
    param_list = [element for element in param_list if not pd.isnull(element)]
    param_dict[p] = param_list
cur_datetime = datetime.today()


tabulator = pn.widgets.Tabulator(
    value=pd.DataFrame(
        columns=[
            "project",
            "session",
            "scan_start",
            "scan_number",
            "observer",
            "frontend",
            "backend",
            "procname",
            "obstype",
            "procscan",
            "proctype",
            "object",
            "script_name",
        ]
    ),
    # selectable="True",
    disabled=True,
    show_index=False,
    pagination="remote",
    page_size=10,
    name="Selected data",
)


def update_tabulator(bounds):
    filtered = current_filtered
    filtered = filtered[
        (filtered["projected_x"] >= bounds[0])
        & (filtered["projected_x"] < bounds[2])
        & (filtered["projected_y"] >= bounds[1])
        & (filtered["projected_y"] < bounds[3])
    ]
    sessions = filtered.index.get_level_values("session").unique().tolist()
    df = pd.DataFrame()
    df = metadata[metadata["Session"].isin(sessions)]
    # df["Archive"] = df.apply(
    #     lambda row: f"""<a href='http://{alda_address}/disk/sessions/{row.Session}/' target='_blank'>
    #         <div title='View in archive'>{LINK_SVG}</div></a>""",
    #     axis=1,
    # )
    df["Archive"] = [
        f"""<a href='http://{alda_address}/disk/sessions/{session}/' target='_blank'>
             <div title='View in archive'>{LINK_SVG}</div></a>"""
        for session in df["Session"]
    ]
    tabulator.value = df
    tabulator.formatters = {"Archive": {"type": "html", "field": "html"}}


def crosshair_info(x, y):
    pc = crs.PlateCarree()
    ra, dec = pc.transform_point(x, y, crs.Mollweide())
    ra = round(ra, 1)
    dec = round(dec, 1)
    text = hv.Text(
        x,
        y,
        f"RA: {ra}\N{DEGREE SIGN}\nDec: {dec}\N{DEGREE SIGN}",
        halign="left",
        valign="bottom",
    )
    return (
        hv.HLine(y).opts(color="lightblue", line_width=0.5)
        * hv.VLine(x).opts(color="lightblue", line_width=0.5)
        * text
    )


class AntennaPositionExplorer(param.Parameterized):
    cmap = param.Selector(
        default=cm["rainbow4"], objects={c: cm[c] for c in cmaps}, label="Color map"
    )
    project = param.String("")
    session = param.String("")
    observer = param.String("")
    frontend = param.ListSelector(
        default=param_dict["frontend"], objects=param_dict["frontend"]
    )
    backend = param.ListSelector(
        default=param_dict["backend"], objects=param_dict["backend"]
    )
    scan_number = param.Range(default=(0, 5000), bounds=(0, 5000))
    scan_start = param.DateRange(
        default=(datetime(2002, 1, 1), cur_datetime - timedelta(days=1)),
        bounds=(datetime(2002, 1, 1), cur_datetime),
    )
    proc_name = param.String("All")
    obs_type = param.String("All")
    proc_scan = param.String("All")
    proc_type = param.ListSelector(
        default=param_dict["proctype"], objects=param_dict["proctype"]
    )
    obj = param.String("")
    script_name = param.String("")


    @param.depends(
        "project",
        "session",
        "observer",
        "frontend",
        "backend",
        "scan_number",
        "scan_start",
        "proc_name",
        "obs_type",
        "proc_scan",
        "obj",
        "script_name",
    )
    def points(self):
        print("Selecting data...")
        start = time.perf_counter()
        filtered = dataset
        if self.project:
            if self.project in param_dict["project"]:
                filtered = filtered.xs(self.session, level=0, drop_level=False)
            else:
                cur_projects = filtered.index.get_level_values("project")
                filtered_projects = [
                    project
                    for project in param_dict["project"]
                    if self.project in project
                ]
                filtered = filtered[cur_projects.isin(filtered_projects)]
        if self.session:
            checkpoint = time.perf_counter()
            if self.session in param_dict["session"]:
                filtered = filtered.xs(self.session, level=1, drop_level=False)
            else:
                cur_sessions = filtered.index.get_level_values("session")
                filtered_sessions = [
                    session
                    for session in param_dict["session"]
                    if self.session in session
                ]
                filtered = filtered[cur_sessions.isin(filtered_sessions)]
            print(f"Filter by session: {time.perf_counter() - checkpoint}s")
        if self.observer:
            checkpoint = time.perf_counter()
            if self.observer in param_dict["observer"]:
                filtered = filtered.loc[pd.IndexSlice[:, :, :, :, self.observer], :]
            else:
                cur_observers = filtered.index.get_level_values("observer")
                filtered_observers = [
                    observer
                    for observer in param_dict["observer"]
                    if self.observer in observer
                ]
                filtered = filtered[cur_observers.isin(filtered_observers)]
            print(f"Filter by observer: {time.perf_counter() - checkpoint}s")
        if self.proc_name != "All":
            checkpoint = time.perf_counter()
            if self.proc_name in param_dict["procname"]:
                filtered = filtered.loc[
                    pd.IndexSlice[:, :, :, :, :, :, :, self.proc_name], :
                ]
            else:
                cur_proc_names = filtered.index.get_level_values("procname")
                filtered_proc_names = [
                    proc_name
                    for proc_name in param_dict["procname"]
                    if self.proc_name in proc_name
                ]
                filtered = filtered[cur_proc_names.isin(filtered_proc_names)]
            print(f"Filter by proc_name: {time.perf_counter() - checkpoint}s")
        if self.obs_type != "All":
            checkpoint = time.perf_counter()
            if self.obs_type in param_dict["obstype"]:
                filtered = filtered.loc[
                    pd.IndexSlice[:, :, :, :, :, :, :, :, self.obs_type], :
                ]
            else:
                cur_obs_types = filtered.index.get_level_values("obstype")
                filtered_obs_types = [
                    obs_type
                    for obs_type in param_dict["obstype"]
                    if self.obs_type in obs_type
                ]
                filtered = filtered[cur_obs_types.isin(filtered_obs_types)]
            print(f"Filter by obs_type: {time.perf_counter() - checkpoint}s")
        if self.proc_scan != "All":
            checkpoint = time.perf_counter()
            if self.proc_scan in param_dict["procscan"]:
                filtered = filtered.loc[
                    pd.IndexSlice[:, :, :, :, :, :, :, :, :, self.proc_scan], :
                ]
            else:
                cur_proc_scans = filtered.index.get_level_values("procscan")
                filtered_proc_scans = [
                    proc_scan
                    for proc_scan in param_dict["procscan"]
                    if self.proc_scan in proc_scan
                ]
                filtered = filtered[cur_proc_scans.isin(filtered_proc_scans)]
            print(f"Filter by proc_scan: {time.perf_counter() - checkpoint}s")
        if self.scan_number != (0, 5000):
            checkpoint = time.perf_counter()
            scan_numbers = filtered.index.get_level_values("scan_number")
            filtered = filtered[
                (scan_numbers >= self.scan_number[0])
                & (scan_numbers < self.scan_number[1])
            ]
            print(f"Filter by scan_number: {time.perf_counter() - checkpoint}s")
        if self.scan_start != (datetime(2002, 1, 1), cur_datetime - timedelta(days=1)):
            checkpoint = time.perf_counter()
            scan_starts = filtered.index.get_level_values("scan_start")
            filtered = filtered[
                (scan_starts >= self.scan_start[0]) & (scan_starts < self.scan_start[1])
            ]
            print(f"Filter by scan_start: {time.perf_counter() - checkpoint}s")
        if self.frontend != param_dict["frontend"]:
            checkpoint = time.perf_counter()
            cur_frontends = filtered.index.get_level_values("frontend")
            filtered = filtered[cur_frontends.isin(self.frontend)]
            print(f"Filter by frontend: {time.perf_counter() - checkpoint}s")
        if self.backend != param_dict["backend"]:
            checkpoint = time.perf_counter()
            cur_backends = filtered.index.get_level_values("backend")
            filtered = filtered[cur_backends.isin(self.backend)]
            print(f"Filter by backend: {time.perf_counter() - checkpoint}s")
        if self.proc_type != param_dict["proctype"]:
            checkpoint = time.perf_counter()
            cur_proc_types = filtered.index.get_level_values("proctype")
            filtered = filtered[cur_proc_types.isin(self.proc_type)]
            print(f"Filter by proc_type: {time.perf_counter() - checkpoint}s")
        if self.obj:
            checkpoint = time.perf_counter()
            if self.obj in param_dict["object"]:
                filtered = filtered.xs(self.session, level=11, drop_level=False)
            else:
                cur_objects = filtered.index.get_level_values("object")
                filtered_objects = [
                    obj for obj in param_dict["object"] if self.obj in obj
                ]
                filtered = filtered[cur_objects.isin(filtered_objects)]
            print(f"Filter by object: {time.perf_counter() - checkpoint}s")
        if self.script_name:
            checkpoint = time.perf_counter()
            if self.script_name in param_dict["script_name"]:
                filtered = filtered.xs(self.script_name, level=1, drop_level=False)
            else:
                cur_script_names = filtered.index.get_level_values("script_name")
                filtered_script_names = [
                    script_name
                    for script_name in param_dict["script_name"]
                    if self.script_name in script_name
                ]
                filtered = filtered[cur_script_names.isin(filtered_script_names)]
            print(f"Filter by script_name: {time.perf_counter() - checkpoint}s")
        print(f"Elapsed time: {time.perf_counter() - start}s")
        current_filtered = filtered
        points = gv.Points(
            filtered,
            kdims=["projected_x", "projected_y"],
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
        bounds = hv.DynamicMap(
            lambda bounds: hv.Bounds(bounds).opts(color="DarkCyan"), streams=[box]
        )
        box_select_tool = BoxSelectTool(
            # persistent=True,
            # overlay=BoxAnnotation(fill_color="#add8e6", fill_alpha=0.4),
        )

        pointer = streams.PointerXY(x=0, y=0, source=shaded)

        plot = (
            shaded.opts(tools=[box_select_tool])
            * gv.feature.grid()
            * bounds
            * hv.DynamicMap(crosshair_info, streams=[pointer])
        )

        return plot


ant_pos = AntennaPositionExplorer(name="")
widgets = pn.Param(
    ant_pos.param,
    widgets={
        "project": {
            "type": pn.widgets.AutocompleteInput(
                options=param_dict["project"],
                value="",
                restrict=False,
                min_characters=1,
                case_sensitive=False,
                name="Projects",
            )
        },
        "session": {
            "type": pn.widgets.AutocompleteInput(
                options=param_dict["session"],
                value="",
                restrict=False,
                min_characters=1,
                case_sensitive=False,
                name="Sessions",
            )
        },
        "observer": {
            "type": pn.widgets.AutocompleteInput(
                options=param_dict["observer"],
                value="",
                restrict=False,
                min_characters=1,
                case_sensitive=False,
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
                options=["All"] + param_dict["procname"],
                value="All",
                restrict=True,
                min_characters=1,
                case_sensitive=False,
                name="Proc names",
            )
        },
        "obs_type": {
            "type": pn.widgets.AutocompleteInput(
                options=["All"] + param_dict["obstype"],
                value="All",
                restrict=True,
                min_characters=1,
                case_sensitive=False,
                name="Observation types",
            )
        },
        "proc_scan": {
            "type": pn.widgets.AutocompleteInput(
                options=["All"] + param_dict["procscan"],
                value="All",
                restrict=True,
                min_characters=1,
                case_sensitive=False,
                name="Proc scan",
            )
        },
        "obj": {
            "type": pn.widgets.AutocompleteInput(
                options=param_dict["object"],
                value="",
                restrict=False,
                min_characters=1,
                case_sensitive=False,
                name="Observed object",
            )
        },
        "script_name": {
            "type": pn.widgets.AutocompleteInput(
                options=param_dict["script_name"],
                value="",
                restrict=False,
                min_characters=1,
                case_sensitive=False,
                name="Script name",
            )
        },
    },
)


def highlight_clicked(val, color):
    return f"background-color: {color}" if val else None


def filter_session(event):
    session = tabulator.value["Session"].iloc[event.row]
    prev_session = ant_pos.session
    if session == prev_session:
        ant_pos.session = ""
        tabulator.style.applymap(
            highlight_clicked, color="white", subset=[event.row, slice(None)]
        )
    else:
        ant_pos.session = session
        tabulator.style.applymap(
            highlight_clicked, color="lightblue", subset=[event.row, slice(None)]
        )


tabulator.on_click(filter_session)


template = pn.template.BootstrapTemplate(
    title="GBT Antenna Data Interactive Dashboard",
    sidebar=widgets,
    header_background="LightSeaGreen",
)
template.main.append(pn.Column(ant_pos.view(), tabulator))
template.servable()

# to run:
# panel serve ant_pos_panel_server.py --allow-websocket-origin [address]
# --args [full data parquet] [metadata parquet] [alda address]

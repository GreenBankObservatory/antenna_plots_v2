print("Importing...")
from datetime import datetime, timedelta
import time
import argparse

import pandas as pd

import panel as pn
import holoviews as hv
import holoviews.operation.datashader as hd
from holoviews import streams
import geoviews as gv
from cartopy import crs
from colorcet import cm

hv.extension("bokeh")
pn.extension(
    "tabulator", loading_spinner="dots", reuse_sessions="True", throttled="True"
)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("full_data_path")
    parser.add_argument("metadata_path")
    args = parser.parse_args()
    return args.full_data_path, args.metadata_path


@pn.cache
def get_data(full_data_path, metadata_path):
    print("Reading parquet files into memory...")
    start = time.perf_counter()
    dataset = pd.read_parquet(full_data_path)
    metadata = pd.read_parquet(metadata_path)
    print(f"Elapsed time: {time.perf_counter() - start}s")
    return dataset, metadata


# TODO: add 'help' or descriptions for filters

# TODO: ask about metadata - multiple backends, procnames, obstypes, etc. per session?
# TODO: find out ways to prevent insane load time in beginning?
# TODO: code better


full_data_path, metadata_path = parse_arguments()
dataset, metadata = get_data(full_data_path, metadata_path)
cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"] # color maps

# Generate lists of unique values for each parameter
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
    # ignore nans for now
    param_list = [element for element in param_list if not pd.isnull(element)]
    param_dict[p] = param_list

cur_datetime = datetime.today()
pc = crs.PlateCarree()
moll = crs.Mollweide()
prev_selected_session = "" # for tabulator clicking


# Generate widgets
cmap = pn.widgets.Select(
    value=cm["rainbow4"], options={c: cm[c] for c in cmaps}, name="Color map"
)
ra = pn.widgets.RangeSlider(
    start=-180, end=180, step=0.1, value=(-180, 180), name="Right ascension (J2000)"
)
dec = pn.widgets.RangeSlider(
    start=-90, end=90, step=0.1, value=(-90, 90), name="Declination (J2000)"
)
project = pn.widgets.AutocompleteInput(
    options=param_dict["project"],
    value="",
    restrict=False,
    min_characters=1,
    case_sensitive=False,
    name="Projects",
)
session = pn.widgets.AutocompleteInput(
    options=param_dict["session"],
    value="",
    restrict=False,
    min_characters=1,
    case_sensitive=False,
    name="Sessions",
)
observer = pn.widgets.AutocompleteInput(
    options=param_dict["observer"],
    value="",
    restrict=False,
    min_characters=1,
    case_sensitive=False,
    name="Observers",
)
scan_start = pn.widgets.DatetimeRangeInput(
    start=datetime(2002, 1, 1),
    end=cur_datetime,
    value=(datetime(2002, 1, 1), cur_datetime - timedelta(days=1)),
    name="Date and time of first scan",
)
proc_name = pn.widgets.AutocompleteInput(
    options=["All"] + param_dict["procname"],
    value="All",
    restrict=True,
    min_characters=1,
    case_sensitive=False,
    name="Proc names",
)
obs_type = pn.widgets.AutocompleteInput(
    options=["All"] + param_dict["obstype"],
    value="All",
    restrict=True,
    min_characters=1,
    case_sensitive=False,
    name="Observation types",
)
proc_scan = pn.widgets.AutocompleteInput(
    options=["All"] + param_dict["procscan"],
    value="All",
    restrict=True,
    min_characters=1,
    case_sensitive=False,
    name="Proc scan",
)
obj = pn.widgets.AutocompleteInput(
    options=param_dict["object"],
    value="",
    restrict=False,
    min_characters=1,
    case_sensitive=False,
    name="Observed object",
)
script_name = pn.widgets.AutocompleteInput(
    options=param_dict["script_name"],
    value="",
    restrict=False,
    min_characters=1,
    case_sensitive=False,
    name="Script name",
)
frontend = pn.widgets.MultiSelect(
    value=param_dict["frontend"], options=param_dict["frontend"], name="Frontend"
)
backend = pn.widgets.MultiSelect(
    value=param_dict["backend"], options=param_dict["backend"], name="Backend"
)
proc_type = pn.widgets.MultiSelect(
    value=param_dict["proctype"], options=param_dict["proctype"], name="Proc type"
)
scan_number = pn.widgets.IntRangeSlider(
    start=0, end=5000, step=1, value=(0, 5000), name="# Scans"
)

tabulator = pn.widgets.Tabulator(
    value=pd.DataFrame(
        columns=[
            "Project",
            "Session",
            "Scan start",
            "# of Scans",
            "Observer",
            "Frontend",
            "Backend",
            "ProcName",
            "Obs type",
            "ProcScan",
            "ProcType",
            "Object",
            "Script name",
            "Archive",
        ]
    ),
    disabled=True,
    show_index=False,
    pagination="remote",
    page_size=15,
    name="Filtered data",
)

def reset_coords(event):
    ra.value = (-180, 180)
    dec.value = (-90, 90)

reset = pn.widgets.Button(name='Reset coordinates')
reset.on_click(reset_coords)


widgets = [
    cmap,
    ra,
    dec,
    reset,
    project,
    session,
    observer,
    frontend,
    backend,
    scan_number,
    scan_start,
    proc_name,
    obs_type,
    proc_scan,
    proc_type,
    obj,
    script_name,
]


@pn.depends(
    ra=ra,
    dec=dec,
    project=project,
    session=session,
    observer=observer,
    frontend=frontend,
    backend=backend,
    scan_number=scan_number,
    scan_start=scan_start,
    proc_name=proc_name,
    obs_type=obs_type,
    proc_scan=proc_scan,
    proc_type=proc_type,
    obj=obj,
    script_name=script_name,
)
def plot_points(
    ra,
    dec,
    project,
    session,
    observer,
    frontend,
    backend,
    scan_number,
    scan_start,
    proc_name,
    obs_type,
    proc_scan,
    proc_type,
    obj,
    script_name,
    **kwargs,
):
    """Filter dataframe based on widget values and generate Geoviews points"""
    print("Selecting data...")
    start = time.perf_counter()
    filtered = dataset
    if ra != (-180, 180):
        checkpoint = time.perf_counter()
        ras = filtered.index.get_level_values("RAJ2000")
        filtered = filtered[
            (ras >= ra[0]) & (ras < ra[1])
        ]
        print(f"Filter by ra: {time.perf_counter() - checkpoint}s")
    if dec != (-90, 90):
        checkpoint = time.perf_counter()
        decs = filtered.index.get_level_values("DECJ2000")
        filtered = filtered[
            (decs >= dec[0]) & (decs < dec[1])
        ]
        print(f"Filter by dec: {time.perf_counter() - checkpoint}s")
    if project:
        checkpoint = time.perf_counter()
        if project in param_dict["project"]:
            filtered = filtered.xs(project, level=0, drop_level=False)
        else:
            cur_projects = filtered.index.get_level_values("project")
            filtered_projects = [
                project for project in param_dict["project"] if project in project
            ]
            filtered = filtered[cur_projects.isin(filtered_projects)]
        print(f"Filter by project: {time.perf_counter() - checkpoint}s")
    if session:
        checkpoint = time.perf_counter()
        if session in param_dict["session"]:
            filtered = filtered.xs(session, level=1, drop_level=False)
        else:
            cur_sessions = filtered.index.get_level_values("session")
            filtered_sessions = [
                session for session in param_dict["session"] if session in session
            ]
            filtered = filtered[cur_sessions.isin(filtered_sessions)]
        print(f"Filter by session: {time.perf_counter() - checkpoint}s")
    if observer:
        checkpoint = time.perf_counter()
        if observer in param_dict["observer"]:
            filtered = filtered.loc[pd.IndexSlice[:, :, :, :, observer], :]
        else:
            cur_observers = filtered.index.get_level_values("observer")
            filtered_observers = [
                observer for observer in param_dict["observer"] if observer in observer
            ]
            filtered = filtered[cur_observers.isin(filtered_observers)]
        print(f"Filter by observer: {time.perf_counter() - checkpoint}s")
    if proc_name != "All":
        checkpoint = time.perf_counter()
        if proc_name in param_dict["procname"]:
            filtered = filtered.loc[pd.IndexSlice[:, :, :, :, :, :, :, proc_name], :]
        else:
            cur_proc_names = filtered.index.get_level_values("procname")
            filtered_proc_names = [
                proc_name
                for proc_name in param_dict["procname"]
                if proc_name in proc_name
            ]
            filtered = filtered[cur_proc_names.isin(filtered_proc_names)]
        print(f"Filter by proc_name: {time.perf_counter() - checkpoint}s")
    if obs_type != "All":
        checkpoint = time.perf_counter()
        if obs_type in param_dict["obstype"]:
            filtered = filtered.loc[pd.IndexSlice[:, :, :, :, :, :, :, :, obs_type], :]
        else:
            cur_obs_types = filtered.index.get_level_values("obstype")
            filtered_obs_types = [
                obs_type for obs_type in param_dict["obstype"] if obs_type in obs_type
            ]
            filtered = filtered[cur_obs_types.isin(filtered_obs_types)]
        print(f"Filter by obs_type: {time.perf_counter() - checkpoint}s")
    if proc_scan != "All":
        checkpoint = time.perf_counter()
        if proc_scan in param_dict["procscan"]:
            filtered = filtered.loc[
                pd.IndexSlice[:, :, :, :, :, :, :, :, :, proc_scan], :
            ]
        else:
            cur_proc_scans = filtered.index.get_level_values("procscan")
            filtered_proc_scans = [
                proc_scan
                for proc_scan in param_dict["procscan"]
                if proc_scan in proc_scan
            ]
            filtered = filtered[cur_proc_scans.isin(filtered_proc_scans)]
        print(f"Filter by proc_scan: {time.perf_counter() - checkpoint}s")
    if scan_number != (0, 5000):
        checkpoint = time.perf_counter()
        scan_numbers = filtered.index.get_level_values("scan_number")
        filtered = filtered[
            (scan_numbers >= scan_number[0]) & (scan_numbers < scan_number[1])
        ]
        print(f"Filter by scan_number: {time.perf_counter() - checkpoint}s")
    if scan_start != (datetime(2002, 1, 1), cur_datetime - timedelta(days=1)):
        checkpoint = time.perf_counter()
        scan_starts = filtered.index.get_level_values("scan_start")
        filtered = filtered[
            (scan_starts >= scan_start[0]) & (scan_starts < scan_start[1])
        ]
        print(f"Filter by scan_start: {time.perf_counter() - checkpoint}s")
    if frontend != param_dict["frontend"]:
        checkpoint = time.perf_counter()
        cur_frontends = filtered.index.get_level_values("frontend")
        filtered = filtered[cur_frontends.isin(frontend)]
        print(f"Filter by frontend: {time.perf_counter() - checkpoint}s")
    if backend != param_dict["backend"]:
        checkpoint = time.perf_counter()
        cur_backends = filtered.index.get_level_values("backend")
        filtered = filtered[cur_backends.isin(backend)]
        print(f"Filter by backend: {time.perf_counter() - checkpoint}s")
    if proc_type != param_dict["proctype"]:
        checkpoint = time.perf_counter()
        cur_proc_types = filtered.index.get_level_values("proctype")
        filtered = filtered[cur_proc_types.isin(proc_type)]
        print(f"Filter by proc_type: {time.perf_counter() - checkpoint}s")
    if obj:
        checkpoint = time.perf_counter()
        if obj in param_dict["object"]:
            filtered = filtered.xs(obj, level=11, drop_level=False)
        else:
            cur_objects = filtered.index.get_level_values("object")
            filtered_objects = [obj for obj in param_dict["object"] if obj in obj]
            filtered = filtered[cur_objects.isin(filtered_objects)]
        print(f"Filter by object: {time.perf_counter() - checkpoint}s")
    if script_name:
        checkpoint = time.perf_counter()
        if script_name in param_dict["script_name"]:
            filtered = filtered.xs(script_name, level=1, drop_level=False)
        else:
            cur_script_names = filtered.index.get_level_values("script_name")
            filtered_script_names = [
                script_name
                for script_name in param_dict["script_name"]
                if script_name in script_name
            ]
            filtered = filtered[cur_script_names.isin(filtered_script_names)]
        print(f"Filter by script_name: {time.perf_counter() - checkpoint}s")
    print(f"Elapsed time: {time.perf_counter() - start}s")
    points = gv.Points(
        filtered,
        kdims=["projected_x", "projected_y"],
        crs=moll,
    )
    points = points.opts(
        gv.opts.Points(
            projection=moll, global_extent=True, width=800, height=400
        )
    )

    update_tabulator(filtered) # TODO: move somewhere better?

    return points


@pn.depends(cmap)
def view(cmap, **kwargs):
    """Creates the plot that updates with widget values"""

    points = hv.DynamicMap(plot_points)
    agg = hd.rasterize(
        points,
        x_sampling=1,
        y_sampling=1,
    )
    shaded = hd.shade(agg, cmap=cmap).opts(
        projection=moll,
        global_extent=True,
        width=800,
        height=400,
    )

    box = streams.BoundsXY(source=shaded, bounds=(0, 0, 0, 0))
    box.add_subscriber(update_ra_dec)

    pointer = streams.PointerXY(x=0, y=0, source=shaded) # for crosshair

    plot = (
        shaded.opts(tools=["box_select"])
        * gv.feature.grid()
        * hv.DynamicMap(crosshair, streams=[pointer])
    )

    return plot


def update_ra_dec(bounds):
    """Changes the RA/Dec widgets based on bounds returned by BoxSelectTool"""
    mleft, mbottom, mright, mtop = bounds # bounds in Mollweide projection

    # Find four corners of rectangle in PlateCarree projection
    pc_left_bottom = pc.transform_point(mleft, mbottom, moll)
    pc_left_top = pc.transform_point(mleft, mtop, moll)
    pc_right_bottom = pc.transform_point(mright, mbottom, moll)
    pc_right_top = pc.transform_point(mright, mtop, moll)

    # Projection distorts RA, so take widest range
    pc_left = min(pc_left_bottom[0], pc_left_top[0])
    pc_right = max(pc_right_bottom[0], pc_right_top[0])

    # Dec doesn't vary
    pc_bottom = pc_left_bottom[1]
    pc_top = pc_left_top[1]

    # Update widgets
    ra.value = (pc_left, pc_right)
    dec.value = (pc_bottom, pc_top)


def update_tabulator(filtered):
    """Updates tabulator based on current filtered dataframe"""
    sessions = filtered.index.get_level_values("session").unique().tolist()
    df = metadata[metadata["Session"].isin(sessions)]
    tabulator.value = df
    tabulator.formatters = {"Archive": {"type": "html", "field": "html"}}


def filter_session(event):
    """Called when the tabulator registers a click event. Changes the session filter to the clicked value"""
    cur_session = tabulator.value["Session"].iloc[event.row]
    prev_selected_session = session.value
    if cur_session == prev_selected_session:
        session.value = ""
    else:
        session.value = cur_session


tabulator.on_click(filter_session) # Registers callback


def crosshair(x, y):
    """Generates crosshair and label from coordinates of PointerXY stream"""
    ra, dec = pc.transform_point(x, y, moll)
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


template = pn.template.BootstrapTemplate(
    title="GBT Antenna Data Interactive Dashboard",
    sidebar=widgets,
    header_background="LightSeaGreen",
)
template.main.append(pn.Column(view, tabulator))
template.servable()

# to run:
# panel serve ant_pos_panel_server.py --allow-websocket-origin [address]
# --args [full data parquet] [metadata parquet]

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


# TODO: reset all filters button?


full_data_path, metadata_path = parse_arguments()
dataset, metadata = get_data(full_data_path, metadata_path)
cmaps = ["rainbow4", "bgy", "bgyw", "bmy", "gray", "kbc"]  # color maps

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
prev_selected_session = ""  # for tabulator clicking

default_ranges = {
    "RAJ2000": (-180, 180),
    "DECJ2000": (-90, 90),
    "scan_number": (0, 5000),
    "scan_start": (datetime(2002, 1, 1), cur_datetime - timedelta(days=1)),
}


# Generate widgets
cmap = pn.widgets.Select(
    value=cm["rainbow4"], options={c: cm[c] for c in cmaps}, name="Color map"
)
RAJ2000 = pn.widgets.RangeSlider(
    start=-180, end=180, step=0.1, value=(-180, 180), name="Right ascension (J2000)"
)
DECJ2000 = pn.widgets.RangeSlider(
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
procname = pn.widgets.AutocompleteInput(
    options=["All"] + param_dict["procname"],
    value="All",
    restrict=True,
    min_characters=1,
    case_sensitive=False,
    name="Proc names",
)
obstype = pn.widgets.AutocompleteInput(
    options=["All"] + param_dict["obstype"],
    value="All",
    restrict=True,
    min_characters=1,
    case_sensitive=False,
    name="Observation types",
)
procscan = pn.widgets.AutocompleteInput(
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
proctype = pn.widgets.MultiSelect(
    value=param_dict["proctype"], options=param_dict["proctype"], name="Proc type"
)
scan_number = pn.widgets.IntRangeSlider(
    start=0, end=5000, step=1, value=(0, 5000), name="Scan Number"
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
    RAJ2000.value = (-180, 180)
    DECJ2000.value = (-90, 90)


def about_callback(event):
    template.open_modal()


reset = pn.widgets.Button(name="Reset coordinates")
reset.on_click(reset_coords)

modal_btn = pn.widgets.Button(name="Click for more information")
modal_btn.on_click(about_callback)


widgets = [
    modal_btn,
    cmap,
    RAJ2000,
    DECJ2000,
    reset,
    project,
    session,
    observer,
    frontend,
    backend,
    scan_number,
    scan_start,
    procname,
    obstype,
    procscan,
    proctype,
    obj,
    script_name,
]


@pn.depends(
    RAJ2000=RAJ2000,
    DECJ2000=DECJ2000,
    project=project,
    session=session,
    observer=observer,
    frontend=frontend,
    backend=backend,
    scan_number=scan_number,
    scan_start=scan_start,
    procname=procname,
    obstype=obstype,
    procscan=procscan,
    proctype=proctype,
    obj=obj,
    script_name=script_name,
)
def plot_points(
    RAJ2000,
    DECJ2000,
    project,
    session,
    observer,
    frontend,
    backend,
    scan_number,
    scan_start,
    procname,
    obstype,
    procscan,
    proctype,
    obj,
    script_name,
):
    """Filter dataframe based on widget values and generate Geoviews points"""
    print("Selecting data...")
    start = time.perf_counter()
    filtered = dataset
    param_types = {
        "autocomplete_unrestricted": ["project", "session", "observer", "object", "script_name"],
        "autocomplete_restricted": ["procname", "obstype", "procscan"],
        "multiselect": ["frontend", "backend", "proctype"],
        "range": ["RAJ2000", "DECJ2000", "scan_start", "scan_number"],
    }
    params = {
        "project": project,
        "session": session,
        "observer": observer,
        "object": obj,
        "script_name": script_name,
        "procname": procname,
        "obstype": obstype,
        "procscan": procscan,
        "frontend": frontend,
        "backend": backend,
        "proctype": proctype,
        "RAJ2000": RAJ2000,
        "DECJ2000": DECJ2000,
        "scan_start": scan_start,
        "scan_number": scan_number,
    }

    for param_name in param_types["autocomplete_unrestricted"]:
        param = params[param_name]
        if param:
            checkpoint = time.perf_counter()
            if param in param_dict[param_name]:
                filtered = filtered.xs(param, level=param_name, drop_level=False)
            else:
                cur_values = filtered.index.get_level_values(param_name)
                filtered_values = [
                    element for element in param_dict[param_name] if param in element
                ]
                filtered = filtered[cur_values.isin(filtered_values)]
            print(f"Filter by {param_name}: {time.perf_counter() - checkpoint}s")

    for param_name in param_types["autocomplete_restricted"]:
        param = params[param_name]
        if param != "All":
            checkpoint = time.perf_counter()
            filtered = filtered.xs(param, level=param_name, drop_level=False)
            print(f"Filter by {param_name}: {time.perf_counter() - checkpoint}s")

    for param_name in param_types["multiselect"]:
        param = params[param_name]
        if param != param_dict[param_name]:
            checkpoint = time.perf_counter()
            cur_values = filtered.index.get_level_values(param_name)
            filtered = filtered[cur_values.isin(param)]
            print(f"Filter by {param_name}: {time.perf_counter() - checkpoint}s")

    for param_name in param_types["range"]:
        param = params[param_name]
        if param != default_ranges[param_name]:
            checkpoint = time.perf_counter()
            cur_values = filtered.index.get_level_values(param_name)
            filtered = filtered[(cur_values >= param[0]) & (cur_values < param[1])]
            print(f"Filter by {param_name}: {time.perf_counter() - checkpoint}s")

    print(f"Elapsed time: {time.perf_counter() - start}s")
    points = gv.Points(
        filtered,
        kdims=["projected_x", "projected_y"],
        crs=moll,
    )
    points = points.opts(
        gv.opts.Points(projection=moll, global_extent=True, width=800, height=400)
    )

    update_tabulator(filtered)  # TODO: move somewhere better?

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
    spread = hd.dynspread(shaded)

    box = streams.BoundsXY(source=shaded, bounds=(0, 0, 0, 0))
    box.add_subscriber(update_ra_dec)

    pointer = streams.PointerXY(x=0, y=0, source=shaded)  # for crosshair

    plot = (
        spread.opts(tools=["box_select"], active_tools=["pan", "wheel_zoom"])
        * gv.feature.grid()
        * hv.DynamicMap(crosshair, streams=[pointer])
    )

    return plot


def update_ra_dec(bounds):
    """Changes the RA/Dec widgets based on bounds returned by BoxSelectTool"""
    mleft, mbottom, mright, mtop = bounds  # bounds in Mollweide projection

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
    RAJ2000.value = (pc_left, pc_right)
    DECJ2000.value = (pc_bottom, pc_top)


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


tabulator.on_click(filter_session)  # Registers callback


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
    ).opts(color="#0072B5")
    return (
        hv.HLine(y).opts(color="lightblue", line_width=0.5)
        * hv.VLine(x).opts(color="lightblue", line_width=0.5)
        * text
    )


text = """
### Introduction
This dashboard is a visual tool to explore and interact with archived GBT data. 
The plot shows GBT antenna positions in the sky with color mapped to density of points. 
Applying filters will update the plot and data table with the corresponding GBT sessions. 
Beware of bugs! Also, no guarantees are made about the accuracy of the displayed information.

### Features
- The project, session, observer, object, and script name widgets allow you to filter by substring (e.g. 'Armen' will also return 'Armentrout')
- To reset the proc name, observation type, or proc scan widgets, choose the option called 'All'
- Click on a row in the data table to filter by that session. To undo that, click again on the same row
- The last column of the table is a link to the corresponding page in the GBT archive
- Use the box select tool (dashed box button to the right of the plot) to filter by a rectangular region that you draw on the plot
- You can toggle between light/dark theme. *However*, this will reset the widgets and reload the page

### Known bug
Sometimes, the plot gets filled with a solid color. If that happens, try removing a filter. If the problem persists, reload the page. 
This could happen if you apply filters such that there is no corresponding data, or if you use the box select tool on a region outside the extents of the plot.
\n
---
"""

template = pn.template.FastListTemplate(
    title="GBT Antenna Data Interactive Dashboard",
    sidebar=widgets,
    logo="https://greenbankobservatory.org/wp-content/uploads/2019/10/GBO-Primary-HighRes-White.png",
)
template.main.append(pn.Column(view, tabulator))
template.modal.append(text)
template.servable()

# to run:
# panel serve ant_pos_panel_server.py --allow-websocket-origin [address]
# --args [full data parquet] [metadata parquet]

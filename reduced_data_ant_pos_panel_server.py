print("Importing...")
from datetime import datetime
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


full_data_path, metadata_path = parse_arguments()
dataset, metadata = get_data(full_data_path, metadata_path)
cmaps = ["rainbow4", "blues", "bgy", "bmy", "kbc"]  # color maps

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
    "GAL_LONG": (-180, 180),
    "GAL_LAT": (-90, 90),
    "scan_number": (0, 5000),
    "scan_start": (datetime(2002, 1, 1), cur_datetime),
}

coord_sys_dict = {
    "Equatorial (J2000)": ["Right ascension (degrees)", "Declination (degrees)"],
    "Galactic": ["Galactic longitude (degrees)", "Galactic latitude (degrees)"],
}


# Generate widgets
cmap = pn.widgets.Select(
    value=cm["rainbow4"], options={c: cm[c] for c in cmaps}, name="Color map"
)
coord_sys = pn.widgets.Select(
    value="Equatorial (J2000)",
    options=["Equatorial (J2000)", "Galactic"],
    name="Coordinate system",
)
longitude_bounds = pn.widgets.RangeSlider(
    start=-180, end=180, step=0.1, value=(-180, 180), name="Right ascension (degrees)"
)
latitude_bounds = pn.widgets.RangeSlider(
    start=-90, end=90, step=0.1, value=(-90, 90), name="Declination (degrees)"
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
    value=(datetime(2002, 1, 1), cur_datetime),
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
    value=metadata,
    groupby=["Session"],
    formatters={"Archive": {"type": "html", "field": "html"}},
    disabled=True,
    show_index=True,
    pagination="remote",
    page_size=50,
    name="Filtered data",
    sizing_mode="stretch_width",
)


def reset_coords(event):
    longitude_bounds.value = (-180, 180)
    latitude_bounds.value = (-90, 90)


def about_callback(event):
    template.open_modal()


def long_callback(target, event):
    target.name = coord_sys_dict[event.new][0]
    target.value = (-180, 180)


def lat_callback(target, event):
    target.name = coord_sys_dict[event.new][1]
    target.value = (-90, 90)


modal_btn = pn.widgets.Button(name="Click for more information")
modal_btn.on_click(about_callback)

reset = pn.widgets.Button(name="Reset coordinates")
reset.on_click(reset_coords)

coord_sys.link(longitude_bounds, callbacks={'value': long_callback})
coord_sys.link(latitude_bounds, callbacks={'value': lat_callback})

widgets = [
    modal_btn,
    cmap,
    coord_sys,
    longitude_bounds,
    latitude_bounds,
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
    coord_sys=coord_sys,
    longitude_bounds=longitude_bounds,
    latitude_bounds=latitude_bounds,
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
    coord_sys,
    longitude_bounds,
    latitude_bounds,
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
        "autocomplete_unrestricted": [
            "project",
            "session",
            "observer",
            "object",
            "script_name",
        ],
        "autocomplete_restricted": ["procname", "obstype", "procscan"],
        "multiselect": ["frontend", "backend", "proctype"],
        "range": ["scan_start", "scan_number"],
    }
    if coord_sys == "Equatorial (J2000)":
        param_types["range"] += ["RAJ2000", "DECJ2000"]
    else:
        param_types["range"] += ["GAL_LONG", "GAL_LAT"]

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
        "scan_start": scan_start,
        "scan_number": scan_number,
        "RAJ2000": longitude_bounds,
        "DECJ2000": latitude_bounds,
        "GAL_LONG": longitude_bounds,
        "GAL_LAT": latitude_bounds,
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

    kdims = (
        ["projected_x", "projected_y"]
        if coord_sys == "Equatorial (J2000)"
        else ["projected_gal_long", "projected_gal_lat"]
    )
    points = gv.Points(
        filtered,
        kdims=kdims,
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
    box.add_subscriber(update_bounds_widgets)

    pointer = streams.PointerXY(x=0, y=0, source=shaded)  # for cursor coordinate info
    pointer.add_subscriber(update_coord_info)

    plot = (
        spread.opts(tools=["box_select"], active_tools=["pan", "wheel_zoom"])
        * gv.feature.grid()
    )

    return plot


def update_bounds_widgets(bounds):
    """Changes the long/lat widgets based on bounds returned by BoxSelectTool"""
    mleft, mbottom, mright, mtop = bounds  # bounds in Mollweide projection

    # Find four corners of rectangle in PlateCarree projection
    pc_left_bottom = pc.transform_point(mleft, mbottom, moll)
    pc_left_top = pc.transform_point(mleft, mtop, moll)
    pc_right_bottom = pc.transform_point(mright, mbottom, moll)
    pc_right_top = pc.transform_point(mright, mtop, moll)

    # Projection distorts longitude, so take widest range
    pc_left = min(pc_left_bottom[0], pc_left_top[0])
    pc_right = max(pc_right_bottom[0], pc_right_top[0])

    # Latitude doesn't vary
    pc_bottom = pc_left_bottom[1]
    pc_top = pc_left_top[1]

    # Update widgets
    longitude_bounds.value = (pc_left, pc_right)
    latitude_bounds.value = (pc_bottom, pc_top)


coord_info = pn.pane.Str("")


def update_coord_info(x, y):
    long, lat = pc.transform_point(x, y, moll)
    long = round(long, 4)
    lat = round(lat, 4)
    if (
        long == float("inf")
        or long != long  # check if nan
        or lat == float("inf")
        or lat != lat
    ):
        coord_info.object = ""
    else:
        if coord_sys.value == "Equatorial (J2000)":
            coord_info.object = f"RA: {long}\N{DEGREE SIGN}, Dec: {lat}\N{DEGREE SIGN}"
        else:
            coord_info.object = (
                f"Gal long: {long}\N{DEGREE SIGN}, Gal lat: {lat}\N{DEGREE SIGN}"
            )


def update_tabulator(filtered):
    """Updates tabulator based on current filtered dataframe"""

    if filtered.equals(dataset):
        tabulator.value = metadata

    else:
        start = time.perf_counter()
        scan_starts = filtered.index.get_level_values("scan_start").unique().tolist()
        print(f"Getting scan starts: {time.perf_counter() - start}s")

        scan_index = metadata.index.get_level_values("Scan start")

        start = time.perf_counter()
        tabulator.value = metadata[scan_index.isin(scan_starts)]
        print(f"Creating filtered metadata table: {time.perf_counter() - start}s")


def filter_session(event):
    """Called when the tabulator registers a click event. Changes the session filter to
    the clicked value"""
    cur_session = tabulator.value.iloc[event.row].name[1]
    prev_selected_session = session.value
    if cur_session == prev_selected_session:
        session.value = ""
    else:
        session.value = cur_session


tabulator.on_click(filter_session)  # Registers callback

text = """
### Introduction
This dashboard is a visual tool to explore and interact with archived GBT data. 
The plot shows GBT antenna positions in the sky with color mapped to density of points. 
Applying filters will update the plot and data table with the corresponding GBT 
sessions. 
Beware of bugs! Also, no guarantees are made about the accuracy of the displayed 
information.

### Features/how to use
- The project, session, observer, object, and script name widgets allow you to filter by
 substring (e.g. 'Armen' will also return 'Armentrout')
- To reset the proc name, observation type, or proc scan widgets, choose the option 
called 'All'
- Click on a row in the data table to filter by that session (*not* scan). To undo 
that, click again on the same row. Note: all the archive links currently point to the 
session page, not the scan page.
- The last column of the table is a link to the corresponding page in the GBT archive
- Use the box select tool (dashed box button to the right of the plot) to filter by a 
rectangular region that you draw on the plot. To reset the box tool, click on the 
"Reset coordinates" button on the sidebar.
- You can toggle between light/dark theme using the button on the upper right corner. 
*However*, this will reset the widgets and reload the page

### Known bugs
- When switching to galactic coordinates, the whole plot will not render until you zoom
or pan slightly.
- Sometimes, the plot gets filled with a solid color. If that happens, try removing a 
filter. If the problem persists, reload the page. This could happen if you apply 
filters such that there is no corresponding data, or if you use the box select tool on 
a region outside the extents of the plot.
\n
---
"""

template = pn.template.FastListTemplate(
    title="GBT Antenna Data Interactive Dashboard",
    sidebar=widgets,
    logo="https://greenbankobservatory.org/wp-content/uploads/2019/10/GBO-Primary-HighRes-White.png",
)
template.main.append(pn.Column(view, coord_info, tabulator, sizing_mode="stretch_both"))
template.modal.append(text)
template.servable()

# to run:
# panel serve ant_pos_panel_server.py --allow-websocket-origin [address]
# --args [full data parquet] [metadata parquet]

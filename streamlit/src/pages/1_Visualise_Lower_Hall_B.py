import asyncio
import os
import sys
import time as t
from typing import Any

import folium 
import geopandas as gpd
import mapclassify
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit_folium as st_folium
import utils.config as configutils
import utils.data_processor as proc
import utils.databricks as dbutils
from folium import Map
from folium.features import GeoJsonTooltip
from geopandas import GeoDataFrame
from shapely.geometry import LinearRing, Polygon
from shapely.ops import unary_union
from utils.init_app_session import app_setup_on_load, init_db_connection, init_state

import streamlit as st

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))


@st.cache_data()
def update_center_location(center: list[float] | None = None):
    if center != None and center != st.session_state.center_loc:
        st.session_state.center_loc = center


def hex_to_rgb(hex_value):
    hex_value = hex_value.strip('#')
    return tuple(int(hex_value[i:i+2], 16) for i in (0, 2, 4))


def map_fmz_colors(feature, meter: bool, fmz: str | None = None):

    if meter:
        color_map = {
            "ZSEWRD": "cadetblue",
            "ZDARNH": "purple",
            "ZUPSHB": "darkgreen",
            "ZHAILY": "lightgreen",
            "ZEXGGT": "pink",
            "ZHAILB": "lightred",
            "ZDARNP": "lightblue",
            "ZHODDN": "darkred",
            "ZDARNB": "darkpurple"
        }
        # fmz_value = feature["properties"]["FMZ1CODE"]
        fmz_value = fmz
        return color_map[str(fmz_value)]
    else:
        color_map = {"ZSEWRD": "#0FFF95",
                     "ZDARNH": "#44A1A0",
                     "ZUPSHB": "#78CDD7",
                     "ZHAILY": "#8AF3FF",
                     "ZEXGGT": "#247B7B",
                     "ZHAILB": "#B1E5F2",
                     "ZDARNP": "#C2F8CB",
                     "ZHODDN": "#B3E9C7",
                     "ZDARNB": "#F0FFF1"}
        fmz_value = feature["properties"]["FMZCODE"]
        return color_map[str(fmz_value)]


def get_fmz_regions(base: GeoDataFrame, fmz_names: list[str]):
    # based on the list of FMZs, group the dataset into FMZs
    # return all the FMZs as their own dataframe
    fmz_regions = {}
    unique_fmz_names = base['FMZCODE'].unique()
    # print(unique_fmz_names)
    for fmz in unique_fmz_names:
        fmz_region = base[base['FMZCODE'] == fmz].copy()
        # fmz_region['polygon']
        fmz_regions[fmz] = fmz_region

    # return the fmz regions
    return fmz_regions


def calculate_centroid(gpd: GeoDataFrame):
    """
    Calculates the centroid for a dataframe 
    Purpose is to provide a central location of all the coordinates in a given dataframe object 

    In order to calculate the centroid the geodataframe is projected to a flat surface, and then the centroid is calculated 
        and then is converted back to the original coordinate system
    Args:
        gpd (GeoDataFrame): _description_
    Returns:
        GeoDataFrame: _description_
    """
    # round tri
    centroid_df = gpd.to_crs('+proj=cea').centroid.to_crs(gpd.crs)
    # calculate the centroids of every geometry point
    # centroid_df = centroid_df.geometry.centroid
    x_sum = 0
    y_sum = 0
    count = 0
    for c in centroid_df:
        x_sum += c.x
        y_sum += c.y
        count += 1

    avg_centroid = [round(y_sum/count, 4), round(x_sum/count, 4)]
    # print(f"Avg Centroid: {avg_centroid}")

    return avg_centroid


def gen_base_layer(center: list[float] | None = None):
    if center:
        # print("location added")
        m = folium.Map(location=center, tiles="cartodb positron")
    else:
        m = folium.Map()
    return m


def add_layer_to_base(base: Map, gdf: GeoDataFrame, fig: folium.Figure):
    # folium.GeoJson(gdf).add_to(base)
    gdf.crs = "EPSG:27700"
    gdf = gdf.to_crs(epsg=4326)

    # print(gdf.crs)
    gdf.explore(m=base)
    # districts_gdf.explore(m=basemap, color='black', style_kwds={
    #                   'fillOpacity': 0.3, 'weight': 0.5},)
    folium.LayerControl().add_to(base)
    fig.add_child(base)
    return fig, base


@st.cache_data()
def load_lower_hall_local():
    # print("Manual Function for loading the Lower Hall B data")
    csv_path = "../data/lower_hall_b_full.csv"
    plain_df = pd.read_csv(csv_path)
    fmz_regions = get_fmz_regions(plain_df, [
                                  "ZSEWRD", "ZDARNH", "ZUPSHB", "ZHAILY", "ZEXGGT", "ZHAILB", "ZDARNP", "ZHODDN", "ZDARNB"])
    lower_hall_b_gdf = proc.df_to_gdf(plain_df, layer_columns=[
                                      'GISID', 'FMZCODE', 'DMACODE', 'PMACODE', 'MAINNAME', 'geometry', 'layer'], geo_type="MultiLineString")
    return lower_hall_b_gdf


# @st.cache_data() # uncomment if the user wishes to only load the network layer data once, it is
def load_network_layer_local(fmz_list):
    """_summary_
    Load the Network Meter DF and only return those FMZs specified in the user input. 

    Args:
        fmz_list (_type_): _description_

    Returns:
        _type_: _description_
    """
    csv_path = "../data/ntwk_meter_full.csv"
    plain_df = pd.read_csv(csv_path)
    filtered = plain_df[plain_df['FMZ1CODE'].isin(fmz_list)]
    ntwkm_gdf = proc.df_to_gdf(filtered, layer_columns=['FMZ1CODE', 'FMZ2CODE', 'DMA1CODE', 'DMA2CODE', 'METRICCALCULATED',
                                                        'GISID', 'TWGUID', 'LIFECYCLESTATUS', 'NETWORKCODE1', 'NETWORKCODE2', 'METERTYPE', 'SHAPEX', 'SHAPEY', 'geometry'], geo_type="Point")

    return ntwkm_gdf


def load_data():
    # print("Load the geospatial data and add layer to the map")
    csv_file = "../data/ne_london_data.csv"
    plain_df = pd.read_csv(csv_file)        # extract our info/data
    ne_london_data = proc.df_to_gdf(plain_df)

    return ne_london_data


def render_base_layer(base_map: Map, lower_hall_gdf: GeoDataFrame) -> Map:
    """
    _summary_
    Create the Mains Pipe Layer

    """
    # update the center location and the boundaries of the base map
    center_loc = calculate_centroid(lower_hall_gdf)
    bounds = lower_hall_gdf.total_bounds
    update_center_location(center=center_loc)
    base_map.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

    # to dynamically add or remove items(e.g. layer) from a map, add them as a feature group and then pass it to st_folium
    lower_hall_fg = folium.FeatureGroup(name="Lower Hall B")

    folium.GeoJson(lower_hall_gdf, style_function=lambda x: {
        "fillColor": '#F35575',
        "color": '#FF0035',
        "weight": 3,
        "fillOpacity": 0.9
    }, name="Lower Hall B Mains Layer", tooltip=GeoJsonTooltip(fields=['MAINNAME', "GISID", "FMZCODE", 'DMACODE'], aliases=['Main Name', 'GISID', 'FMZ Code', 'DMA Code'], labels=True, sticky=False)).add_to(lower_hall_fg)

    # generate an FMZ layer and add it to the base map
    base_map.add_child(lower_hall_fg)
    return base_map


def render_fmz_layer(base_map: Map, lower_hall_gdf: GeoDataFrame, fmz_list: list[str] | None = None) -> Map:
    """_summary_
    This function is used to render the fmz tiles layer
    Args:
        base_map (Map): _description_
        lower_hall_gdf (GeoDataFrame): _description_

    Returns:
        Map: _description_
    """
    fmz_list = ["ZSEWRD", "ZDARNH", "ZUPSHB", "ZHAILY",
                "ZEXGGT", "ZHAILB", "ZDARNP", "ZHODDN", "ZDARNB"]
    # initialize the feature group for the FMZ layers
    fmz_fg = folium.FeatureGroup(name="FMZ Layer")

    for fmz in fmz_list:
        fmz_df = lower_hall_gdf[lower_hall_gdf['FMZCODE'] == fmz]
        # extract multiline string geometries
        fmz_geometries = fmz_df['geometry'].values
        # combine the MultiLineString into a single LineString
        combined_line = unary_union(fmz_geometries)
        polygon = Polygon(combined_line.convex_hull)
        # create geodataframe for ploygon
        polygon_gdf = gpd.GeoDataFrame(geometry=[polygon])
        polygon_gdf.crs = "EPSG:4326"
        polygon_gdf = polygon_gdf.to_crs(epsg=4326)
        polygon_gdf['FMZCODE'] = fmz

        folium.GeoJson(polygon_gdf, style_function=lambda x: {
            "fillColor": map_fmz_colors(x, False),
            "color": '#01295F',
            "weight": 2,
            "fillOpacity": 0.7
        }, name=fmz,
            tooltip=GeoJsonTooltip(fields=["FMZCODE"], aliases=["FMZ Code"], labels=True, sticky=False)).add_to(fmz_fg)

    base_map.add_child(fmz_fg)
    return base_map


def render_ntwk_meter_layer(base_map: Map, ntwkm_gdf: GeoDataFrame, fmz_list: list[str] | None = None) -> folium.FeatureGroup:
    """_summary_

    Args:
        base_map (Map): _description_
        ntwkm_gdf (GeoDataFrame): _description_
        fmz_list (list[str] | None, optional): _description_. Defaults to None.

    Returns:
        folium.FeatureGroup: _description_
    """
    fg_layers = {}

    for fmz in fmz_list:
        # print(f"Current FMZ: {fmz}")
        fg_layers[fmz] = folium.FeatureGroup(
            name=f"{fmz} Network Meter Points")
        fmz_gdf = ntwkm_gdf[ntwkm_gdf['FMZ1CODE'] == fmz]
        hex_color = map_fmz_colors(feature=fmz_gdf, meter=True, fmz=fmz)
        # print(f"Hex Color: {hex_color}")
        for index, row in fmz_gdf.iterrows():
            lat, lon = row['geometry'].y, row['geometry'].x
            meter_marker = folium.Marker(
                location=[lat, lon],
                icon=folium.Icon(color=hex_color, icon='info-sign'),
                popup=f"<h4>Network ID: {row['NETWORKCODE1']} </h4><p>LifeCycle Status: {row['LIFECYCLESTATUS']}</p><p>Metric: {row['METRICCALCULATED']}</p>"
            )
            fg_layers[fmz].add_child(meter_marker)
    return fg_layers

# @st.cache_data()


def request_gis_layer(work_path: str, cluster: int, job_params: Any, f_name: str | None = None) -> pd.DataFrame:
    # # print("Use this function to asynchronously request the data from Databricks")
    # request the data from the databricks api then write the response to the application
    run_id = dbutils.get_one_time_run(db_connection=st.session_state['databricks_connection'],
                                      cluster_id=cluster,
                                      run_name="Get Lower Hall B Layer", timeout_seconds=3600,
                                      workspace_path=work_path, notebook_params=job_params,
                                      git=False)['run_id']
    # while the response status is still false then send another request to the run id
    # print(f"Run ID: {run_id}")
    status = ""
    response = None
    # max_retries = 5
    # count = 0
    while status != "SUCCESS":
        # make a request to the server and get the output of the last run job
        run_response = dbutils.get_job_output(
            db_connection=st.session_state['databricks_connection'], run_id=run_id)
        configutils.save_json_data(
            data=run_response, file_name=f_name)
        state = run_response["metadata"]["state"]["life_cycle_state"]
        if state == "TERMINATED":
            status = run_response["metadata"]["state"]["result_state"]
            if status == "SUCCESS":
                response = run_response
        # add a delay to allow the notebook to complete it's run
        t.sleep(8)

    output_str = response["notebook_output"]["result"]
    # print(f"Output: \n{output_str}")
    # convert the output string to a dictionary
    output_dict = proc.string_to_dict(output_str)
    output_df = pd.DataFrame(output_dict)

    return output_df


@st.cache_data()
def request_ntwk_meter_data(selected_fmz: list[str] | None = None):
    nb_name = 'GISNTWM_Notebook_001'
    nb_path = os.environ.get('AZ_DB_NOTEBOOK_PATH') + nb_name
    param_str = ','.join(selected_fmz)
    # # print(f"Param String: {param_str}")
    ntwk_meter_df = request_gis_layer(work_path=nb_path, cluster=os.environ.get(
        'AZ_DB_CLUSTER_ID'), job_params={"FMZCode": param_str}, f_name="ntwkm_run_output.json")
    st.session_state['ntwk_meter_df'] = ntwk_meter_df
    return ntwk_meter_df


@st.cache_data()
def get_ntwk_session(fmz_list):
    st.session_state['ntwk_meter_df'] = request_ntwk_meter_data(
        selected_fmz=fmz_list)


def main():
    st.markdown(
        """
        <link href="https://rsms.me/inter/inter.css" rel='stylesheet'>
        <h1 style='
        text-align: left; 
        color: white; 
        font-family:Inter;
        '>
        Lower Hall B Map Data</h1>
        """,
        unsafe_allow_html=True,
    )
    st.text(
        'This loads a local dataset of Lower Hall B Mains Layer and Network Meter Layer')
    st.divider()
    st.markdown(
        """
        <link href="https://rsms.me/inter/inter.css" rel='stylesheet'>
        <h3 style=' 
        color: #FEEFDD; 
        font-family:Inter;
        '>
        Lower Hall B FMZs</h3>
        """,
        unsafe_allow_html=True,
    )
    # Load the dataframes
    # gen the data
    lower_hall_gdf = load_lower_hall_local()
    # normalise the crs for our map
    lower_hall_gdf.crs = "EPSG:27700"
    lower_hall_gdf = lower_hall_gdf.to_crs(epsg=4326)
    # # initialise the base map
    base_map = gen_base_layer()
    # st.write(f"{type(st.session_state['ntwk_meter_df'])}")
    # add the fmz regions layer
    fmz_list = ["ZSEWRD", "ZDARNH", "ZUPSHB", "ZHAILY",
                "ZEXGGT", "ZHAILB", "ZDARNP", "ZHODDN", "ZDARNB"]
    # Add the awkward looking fmz regions to the basemap
    # base_map = render_fmz_layer(base_map=base_map, lower_hall_gdf=lower_hall_gdf, fmz_list=fmz_list)
    # add the mains layer
    center_loc = calculate_centroid(lower_hall_gdf)
    # # print(f"Center location: {center_loc}")
    st.session_state['center_loc'] = center_loc
    # add the network meter layer
    
    # folium.LayerControl().add_to(base_map)
    # render the map on screen
    # st_data = st_folium.st_folium(base_map,
    #                               zoom=10,
    #                               width=950, height=560, returned_objects=[])
    # save the map if desired
    if st.button("Save Lower Hall B HTML"):
        # save the map to a file
        base_map.save('../output/LowerHallB_Base.html')
        st.success("Base Map Saved")

    st.divider()

    st.markdown(
        """
        <link href="https://rsms.me/inter/inter.css" rel='stylesheet'>
        <h3 style=' 
        color: #FEEFDD; 
        font-family:Inter;
        '>
        Network Meter Map</h3>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("""
        <link href="https://rsms.me/inter/inter.css" rel='stylesheet'>
        <p style=' 
        color: #FEEFDD; font-family: Inter;
        '>
        Use the dropdown to select the FMZs you want to display on the map.
        </p> """,
                unsafe_allow_html=True,)
    ntwkm_gdf = load_network_layer_local(fmz_list=fmz_list)
    ntwkm_gdf.crs = "EPSG:27700"
    ntwkm_gdf = ntwkm_gdf.to_crs(epsg=4326)

    # Add markers to a Feature Group
    st.session_state['selected_fmzs'] = st.multiselect(
        'Select FMZ Codes', fmz_list)
    st.write(f"Selected FMZ Codes: {st.session_state['selected_fmzs']}")

    # second_map = gen_base_layer()
    base_map = render_base_layer(base_map=base_map, lower_hall_gdf=lower_hall_gdf)
    # ntwk_fg = render_ntwk_meter_layer(base_map=second_map, ntwkm_gdf=ntwkm_gdf, fmz_list=st.session_state['selected_fmzs'])
    ntwk_fg_layers = render_ntwk_meter_layer(
        base_map=base_map, ntwkm_gdf=ntwkm_gdf, fmz_list=st.session_state['selected_fmzs'])

    for key, value in ntwk_fg_layers.items():
        base_map.add_child(value) # 
        
    # center_loc = calculate_centroid(ntwkm_gdf)
    # render the map
    folium.LayerControl().add_to(base_map)
    st_data_two = st_folium.st_folium(
        base_map, center=center_loc, zoom=10, width=950, height=720, returned_objects=[])
    if st.button("Save the Network Meters HTML"):
        # save the map to a file
        base_map.save('../output/NetworkMeter_Base.html')
        st.success("Network Meter Map Saved")
    st.divider()


if __name__ == "__main__":
    app_setup_on_load()
    # init_db_connection()
    init_state()
    # asyncio.run(main())
    main()

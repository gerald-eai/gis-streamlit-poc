# TODO: Try to test if it is possible to request the GIS Mains data, even though it is a large file
# Suggested methods to test this out:
#       A) Save Lower Hall B to DBFS and connect databricks API to the dataset
#       B) Paginate the requested data, or only request per FMZ
from typing import Any
import os
from dotenv import load_dotenv
import time as t
import streamlit as st
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

load_dotenv()
app_setup_on_load()
init_db_connection()
# init_state()


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
    centroid_df = gpd.to_crs('+proj=cea').centroid.to_crs(gpd.crs)
    # calculate the centroids of every geometry point
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


def request_gis_layer(work_path: str, cluster: int, job_params: Any, run_name: str, f_name: str | None = None) -> pd.DataFrame:
    # # print("Use this function to asynchronously request the data from Databricks")
    # request the data from the databricks api then write the response to the application
    run_id = dbutils.get_one_time_run(db_connection=st.session_state['databricks_connection'],
                                      cluster_id=cluster,
                                      run_name=run_name, timeout_seconds=3600,
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
        # use this function to check if the data is received
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


def gen_base_layer(center: list[float] | None = None):
    if center:
        # print("location added")
        m = folium.Map(location=center, tiles="cartodb positron")
    else:
        m = folium.Map()
    return m

@st.cache_data()
def request_ntwk_meter_data(selected_fmz: list[str] | None = None):
    """_summary_
    Call the Network Meter Layers Notebook
    Args:
        selected_fmz (list[str] | None, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    nb_name = 'GISNTWM_Notebook_001'
    nb_path = os.environ.get('AZ_DB_NOTEBOOK_PATH') + nb_name
    print(nb_path, "\n", nb_name)
    param_str = ','.join(selected_fmz)
    # # print(f"Param String: {param_str}")
    ntwk_meter_df = request_gis_layer(work_path=nb_path, cluster=os.environ.get('AZ_DB_CLUSTER_ID'), 
                                      job_params={"FMZCode": param_str}, f_name="ntwkm_run_output.json",
                                      run_name="Get Network Meter Data")
    st.session_state['ntwk_meter_df'] = None
    st.session_state['ntwk_meter_df'] = ntwk_meter_df
    print(f"Network Meter Data: {type(st.session_state['ntwk_meter_df'])}")
    return ntwk_meter_df


@st.cache_data()
def request_mains_data():
    """
    _summary_
    """
    nb_name = 'GISNTWM_Notebook_001'
    nb_path = os.environ.get('AZ_DB_NOTEBOOK_PATH') + nb_name


@st.cache_data()
def load_lower_hall_local():
    # print("Manual Function for loading the Lower Hall B data")
    csv_path = "../data/lower_hall_b_full.csv"
    plain_df = pd.read_csv(csv_path)
    # fmz_regions = get_fmz_regions(plain_df, [
    #                               "ZSEWRD", "ZDARNH", "ZUPSHB", "ZHAILY", "ZEXGGT", "ZHAILB", "ZDARNP", "ZHODDN", "ZDARNB"])

    lower_hall_b_gdf = proc.df_to_gdf(plain_df, layer_columns=[
                                      'GISID', 'FMZCODE', 'DMACODE', 'PMACODE', 'MAINNAME', 'geometry', 'layer'], geo_type="MultiLineString")
    return lower_hall_b_gdf


if __name__ == "__main__":
    st.markdown(
        """
        <link href="https://rsms.me/inter/inter.css" rel='stylesheet'>
        <h1 style='
        text-align: left; 
        color: white; 
        font-family:Inter;
        '>
        Request GIS Data From Databricks</h1>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(""" <p>This page makes requests to the Databricks Cluster using the Databricks REST API to retrieve the GIS Data for Network Meter</p>""", unsafe_allow_html=True)
    st.divider()

    # ------------------------------------------------
    # Request Data from Databricks
    # init variables

    fmz_list = ["ZSEWRD", "ZDARNH", "ZUPSHB", "ZHAILY",
                "ZEXGGT", "ZHAILB", "ZDARNP", "ZHODDN", "ZDARNB"]
    st.markdown(
        """
        <link href="https://rsms.me/inter/inter.css" rel='stylesheet'>
        <h3 style=' 
        color: #FEEFDD; 
        font-family:Inter;
        '>
        Request Network Meter GIS Data from Databricks</h3>
        """,
        unsafe_allow_html=True,
    )
    # Request the Network Meter Layer Data
    if st.button('Fetch Network Meter Layer'):
        # make a request to the data
        request_ntwk_meter_data(selected_fmz=fmz_list)
        # display the data once it's retrieved
        # st.dataframe(st.session_state['ntwk_meter_df'])
        st.success("Network Meter Data requested and saved to cache!")
        # else:
        st.divider()
        st.dataframe(st.session_state['ntwk_meter_df'])

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
    st.warning(
        "Only press the button below if the Network Meter Dataframe above has been successfully requested!")
    st.session_state['selected_fmzs'] = st.multiselect(
        'Select FMZ Codes', fmz_list)
    # st.write(f"Selected FMZ Codes: {st.session_state['selected_fmzs']}")
    if st.button("Render the Map"):
        # convert from df -> gdf
        ntwkm_gdf = proc.df_to_gdf(st.session_state['ntwk_meter_df'], layer_columns=['FMZ1CODE', 'FMZ2CODE', 'DMA1CODE', 'DMA2CODE', 'METRICCALCULATED',
                                                                                     'GISID', 'TWGUID', 'LIFECYCLESTATUS', 'NETWORKCODE1', 'NETWORKCODE2', 'METERTYPE', 'SHAPEX', 'SHAPEY', 'geometry'], geo_type="Point")

        #  convert the crs to EPSG:4236
        ntwkm_gdf.crs = "EPSG:27700"
        ntwkm_gdf = ntwkm_gdf.to_crs(epsg=4326)

        base_map = gen_base_layer()
        ntwk_fg_layers = render_ntwk_meter_layer(
            base_map=base_map, ntwkm_gdf=ntwkm_gdf, fmz_list=st.session_state['selected_fmzs'])
        for key, value in ntwk_fg_layers.items():
            base_map.add_child(value)

        center_loc = calculate_centroid(ntwkm_gdf)
        # render the map
        folium.LayerControl().add_to(base_map)
        st_data_two = st_folium.st_folium(
            base_map, center=center_loc, zoom=10, width=950, height=720, returned_objects=[])
        if st.button("Save the Network Meters HTML"):
            # save the map to a file
            base_map.save('../output/NetworkMeter_Base.html')
            st.success("Network Meter Map Saved")
        st.divider()

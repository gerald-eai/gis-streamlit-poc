# load the base layer geopandas file and return it

"""_summary_

Goal of this module is to make requests using the dbutils module 
and it converts data into the desired format
This would act as our 'Services Layer'

"""
import os
import sys
# import databricks utilitis for development
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shapely.geometry import MultiLineString, Point
from pandas import DataFrame
from geopandas import GeoDataFrame
from dotenv import load_dotenv
from databricks_api import DatabricksAPI
import utils.databricks as dbutils
import utils.config as configutils
import pandas as pd
import geopandas as gpd
import ast
import json

import time as t
from typing import Any
load_dotenv()


def string_to_dict(input_str: str) -> list[str]:
    """_summary_
    This function is used to convert a string in the following format: "['{\"Hello\":1.0, \"World\":\"GPT\"}']" 
    into a list of dictionaries. 
    The string is expected to be a list of dictionaries. 
    The dictionaries are expected to be in the following format: 
    {
        "Hello": 1.0, 
        "World": "GPT"
    }
    The function returns a list of dictionaries. 
    """
    # remove the brackets
    # input_str = input_str.strip("[]")
    # convert the string to a list of dictionaries
    input_list = ast.literal_eval(input_str)
    # convert the list of dictionaries to a list of dictionaries
    input_list = [ast.literal_eval(el) for el in input_list]
    # print(input_list)

    return input_list


def string_to_geometry(geometry_string: str, geo_type: str | None = None) -> Any:
    """
    Convert a string representation of a geometry to a shapely object
    Currently implemented for MultiLineString object, other objects can be applied. 
    """
    if geo_type == "MultiLineString":
        stripped = geometry_string.strip("MULTILINESTRING ()").split(',')
        coords = []
        for el in stripped:
            x, y = float(el.split()[0]), float(el.split()[1])
            coords.append((x, y))
        multi_line_object = MultiLineString([coords])

        return multi_line_object

    if geo_type == "Point":
        stripped_ = geometry_string.strip("POINT ()").split(" ")
        coords = [(round(float(stripped_[0]), 2),
                   round(float(stripped_[1]), 2))]
        point = Point([coords])

        return point


def df_to_gdf(plain_df: DataFrame, geo_type: str, layer_columns: list[str] = None) -> GeoDataFrame:
    """
    Convert a dataframe to a geodataframe
    """
    # extract relevant content from the dataframe
    map_contents = plain_df.copy()
    if layer_columns:
        map_contents = plain_df[layer_columns].copy()
    
    map_contents = map_contents.dropna(axis=1, how='all')
    map_contents['geometry'] = map_contents['geometry'].apply(
        lambda x: string_to_geometry(x, geo_type=geo_type))
    # print(map_contents.head())
    return gpd.GeoDataFrame(map_contents, geometry='geometry')


def load_gdf_from_csv(path: str) -> Any:
    """_summary_
    Read a gdf from a csv file
    Args:
        path (str): _description_

    Returns:
        Any: _description_
    """
    gdf = gpd.read_file(path)
    return gdf

# this is where we create a base class for our app and then we can create a subclass for each service layer


def csv_to_geojson(path: str, output_path: str, geo_type: str, layer_columns: list[str] = None) -> Any:
    """_summary_

    Args:
        path (str): _description_
        output_path (str): _description_
        geo_type (str): _description_

    Returns:
        Any: _description_
    """
    # read the file from csv into dataframe
    df = pd.read_csv(path)
    
    for col in df.columns:
        print(col)

    gdf = df_to_gdf(df, geo_type, layer_columns)
    # gdf = load_gdf_from_csv(path)
    print(gdf.head())

    # save the gdf to a file
    gdf.crs = "EPSG:27700"
    gdf = gdf.to_crs(epsg=4326)  # normalise the data

    gdf.to_file(output_path, driver='GeoJSON')

    return gdf.to_json()


if __name__ == "__main__":
    # path = "data/base_layer.csv"
    # output_path = "data/base_layer.geojson"
    # geo_type = "MultiLineString"
    # csv_to_geojson(path, output_path, geo_type)
    ntwkm_cols = ["ENABLED", "GENID", "GISID", "GLOBALID", "SHORTGISID", "TWGUID", "DATECREATED", "DATEMODIFIED", "WATERTRACEWEIGHT", "GPSX", "GPSY", "GPSZ", "METERTYPE", "DIAMETER", "IMPERIALDIAMETER",
                  "METRICCALCULATED", "FMZ1CODE", "FMZ2CODE", "DMA1CODE", "DMA2CODE", "PMA1CODE", "PMA2CODE", "NETWORKCODE1", "NETWORKCODE2", "geometry", "METERSTATUS", "DATEPOSTED", "SHAPEX", "SHAPEY"]
    mains_cols = ["ENABLED", "CREATIONUSER", "DATECREATED", "DATEMODIFIED", "LASTUSER", "GENID", "GISID", "SHORTGISID", "TWGUID", "OPERATINGPRESSURE", "LIFECYCLESTATUS",
                  "MEASUREDLENGTH", "WATERTRACEWEIGHT", "METRICCALCULATED", "FMZCODE", "DMACODE", "PMACODE", "NETWORKCODE", "WATERTYPE", "geometry", "layer", "OPERATION", "PRESSURETYPE", "GLOBALID"]
    mains_path = '../data/gis_mains.csv'
    ntwkm_path = '../data/gis_wnetworkmeter.csv'

    json_1 = csv_to_geojson(
        mains_path, '../data/GeoJSONs/gis_mains.geojson', 'MultiLineString', mains_cols)
    json_2 = csv_to_geojson(
        ntwkm_path, '../data/GeoJSONs/gis_wnetworkmeter.geojson', 'Point', ntwkm_cols)
    # pass

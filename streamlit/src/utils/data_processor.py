# load the base layer geopandas file and return it

"""_summary_

Goal of this module is to make requests using the dbutils module 
and it converts data into the desired format

"""
import ast
import json
import os
import sys
import time as t
from typing import Any

import geopandas as gpd
import pandas as pd
import utils.config as configutils
import utils.databricks as dbutils
from databricks_api import DatabricksAPI
from dotenv import load_dotenv
from geopandas import GeoDataFrame
from pandas import DataFrame
from shapely.geometry import MultiLineString, Point

# import databricks utilitis for development
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

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
        coords = [(round(float(stripped_[0]),2), round(float(stripped_[1]),2))]
        point = Point([coords])
        
        return point
        
        
def df_to_gdf(plain_df: DataFrame, layer_columns: list[str], geo_type: str) -> GeoDataFrame:
    """
    Convert a dataframe to a geodataframe
    """
    # extract relevant content from the dataframe
    map_contents = plain_df[layer_columns].copy()
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
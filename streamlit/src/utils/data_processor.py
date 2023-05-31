# load the base layer geopandas file and return it

"""_summary_

Goal of this module is to make requests using the dbutils module 
and it converts data into the desired format

"""
import ast
import pandas as pd
import utils.databricks as dbutils
import time as t
from databricks_api import DatabricksAPI
import json
from dotenv import load_dotenv
from shapely.geometry import MultiLineString, Point 
from typing import Any
from geopandas import GeoDataFrame
import geopandas as gpd
from pandas import DataFrame
import utils.config as configutils
import os
import sys
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
    print(input_list)

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
    print(map_contents.head())
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


def request_base_layer(db_connection: DatabricksAPI) -> GeoDataFrame:
    """_summary_
    Request the base layer from databricks and return it as a geodataframe
    Args:
        db_connection (DatabricksAPI): _description_

    Returns:
        GeoDataFrame: _description_
    """
    nb_name = 'GISMAIN_Notebook_002'
    nb_path = os.environ.get('AZ_DB_NOTEBOOK_PATH') + nb_name

    # run the job and return the run id
    run_id = dbutils.get_one_time_run(db_connection=db_connection, cluster_id=os.environ.get('AZ_DB_CLUSTER_ID'),
                                      run_name="Get Lower Hall B Layer", timeout_seconds=3600,
                                      workspace_path=nb_path, notebook_params={
                                          "NE_Area": "LOWERHALLB"},
                                      git=False)['run_id']

    active_jobs = dbutils.list_active_runs(
        db_connection=db_connection, active_only=True)
    configutils.save_json_data(data=active_jobs, file_name='active_jobs.json')

    # allow the job and the run to complete before calling the response
    t.sleep(10)

    run_response = dbutils.get_job_output(
        db_connection=db_connection, run_id=run_id)
    configutils.save_json_data(data=run_response, file_name='active_jobs.json')
    response_output = run_response["notebook_output"]["result"]

    # convert the string to json to a geodataframe
    output_dict = string_to_dict(response_output)
    output_df = pd.DataFrame(output_dict)
    output_gdf = df_to_gdf(plain_df=output_df, layer_columns=[
                           'GISID', 'FMZCODE', 'DMACODE', 'PMACODE', 'MAINNAME', 'WATERTYPE', 'geometry', 'layer'])

    return output_gdf


def load_base_layer_local( file_name: str) -> GeoDataFrame:
    # read from the file
    json_data = configutils.read_from_json(file_name)

    output_str = json_data["notebook_output"]["result"]
    output_dict = string_to_dict(output_str)
    output_df = pd.DataFrame(output_dict)
    output_gdf = df_to_gdf(plain_df=output_df, layer_columns=[
                           'GISID', 'FMZCODE', 'DMACODE', 'PMACODE', 'MAINNAME', 'WATERTYPE', 'geometry', 'layer'], geo_type='Point')
    return output_gdf


def test_base_layer():
    db_connection = dbutils.init_db_connection(
        os.environ.get('AZ_DB_HOST'), os.environ.get('AZ_DB_TOKEN'))
    file_name = "run_output25052023_17H01M16S.json"
    # get_gpd = load_base_layer_local(db_connection=db_connection, file_name=file_name)
    # print(f"Head of the GeoDataFrame: {get_gpd.head()}")

    get_gpd = request_base_layer(db_connection=db_connection)
    print(f"Head of the GeoDataFrame: {get_gpd.head()}")


def test_load_ne_data():
    csv_file = "../data/ne_london_data.csv"
    plain_df = pd.read_csv(csv_file)        # extract our info/data
    ne_london_data = df_to_gdf(plain_df)
    print(ne_london_data.columns)
    # export the data as a geoJSON
    ne_london_data.to_file("../data/ne_london_data.geojson", driver='GeoJSON')
    print(ne_london_data.head())
    # NE London Data is now accessible from the map pages
    # calculate the bounding box of the data


def main():
    test_base_layer()


if __name__ == "__main__":
    print("Loading and Processing the Data Requested from Databricks")
    main()

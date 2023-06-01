# inits the session state of the application
import os 
import streamlit as st 
import utils.data_processor as proc 
import utils.databricks as dbutils
from dotenv import load_dotenv

load_dotenv()


def app_setup_on_load(): 
    st.set_page_config(layout="wide", page_title="GIS Visualisation Tool", page_icon=":earth_africa:", initial_sidebar_state="expanded")
    # initialise the databricks connection with the environment variables 
    
def init_db_connection(): 
    db_connection =  dbutils.init_db_connection(host=os.environ.get("AZ_DB_HOST"), token=os.environ.get("AZ_DB_TOKEN"))
    st.session_state['databricks_connection'] = db_connection
    
def init_state(): 
    st.session_state['zoom_level'] = 5      # start with zoom level 5
    st.session_state['center_loc'] = []     # start with an empty array 
    st.session_state['selected_fmzs'] = []  # input from multiselect 
    # st.session_state['ntwk_meter_df'] = None # requested network meter data
    st.session_state['init_load'] = False
    
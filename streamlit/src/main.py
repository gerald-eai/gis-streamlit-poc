import streamlit as st 
import matplotlib.pyplot as plt
import mapclassify
import folium
import utils.data_processor as dputils 
from utils.init_app_session import app_setup_on_load

if __name__ == '__main__':
    app_setup_on_load()
    st.title('Streamlit Map Visualisation POC')
    st.write(f"The session state type: {type(st.session_state['databricks_connection'])}")
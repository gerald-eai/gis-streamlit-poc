# GIS Streamlit POC 

A quick application that makes use of `streamlit_folium` library to visualise interactive maps in a Streamlit application.

The application renders map data of the GIS Mains and GIS Network Meter datasets that are available on the Databricks cluster. 

**Requirements** 
 - Include a .env file that has the following variables, these variables will initiate a connection to the desired Databricks cluster : 
    - AZ_DB_HOST
    - AZ_DB_TOKEN
    - AZ_DB_CLUSTER_ID
    - AZ_DB_NOTEBOOK_PATH
    - AZ_DB_CLUSTER_HTTP
    - AZ_DB_SQL_PORT

- (Optional) Create a virtual environment to run the application ```python -m venv \path\to\venv```.

Use the _streamlit/src/requirements.txt_ file to install the required packages `pip install -r requirements.txt`. 

To run the application enter the _streamlit/src_ directory and run the following command: `python streamlit run main.py`. 

Current version supports visualisation of local




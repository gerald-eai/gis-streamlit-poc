# GIS Streamlit POC 

A quick application that makes use of `streamlit_folium` and the `databricks_api` library to request and visualise interactive maps in a Streamlit application.

The application renders map data of the GIS Mains and GIS Network Meter datasets that are available on the Databricks cluster. 

**Requirements** 
 - Include a .env file that has the following variables, these variables will initiate a connection to the desired Databricks cluster: *AZ_DB_HOST, AZ_DB_TOKEN, AZ_DB_CLUSTER_ID, AZ_DB_NOTEBOOK_PATH, AZ_DB_CLUSTER_HTTP, AZ_DB_SQL_PORT*
- An example of the .env file is provided below: 
    ```
        file path: /streamlit/src/.env
        AZ_DB_HOST = "Host here"
        AZ_DB_TOKEN= "Your Token here"
        AZ_DB_CLUSTER_ID="Your Cluster ID here"
        AZ_DB_NOTEBOOK_PATH="Your Notebook Path here"
        AZ_DB_CLUSTER_HTTP="Your Cluster HTTP here"
        AZ_DB_SQL_HTTP="Your SQL Compute HTTP here"
        AZ_DB_SQL_PORT=<Your Port Number here>
    
- (Optional) Create a virtual environment to run the application `python -m venv \path\to\venv`

Use the _streamlit/src/requirements.txt_ file to install the required packages `pip install -r requirements.txt`. 

To run the application enter the _streamlit/src_ directory and run the following command: `python streamlit run main.py`. 

Current version supports visualisation of local data and requested Network Meter Data.




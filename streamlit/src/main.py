from utils.init_app_session import (app_setup_on_load, init_db_connection,
                                    init_state)
import streamlit as st

if __name__ == '__main__':
    app_setup_on_load()
    init_db_connection() # uncomment this line to connect to the databricks cluster
    # init_state()
    st.title('Streamlit Map Visualisation POC')
    # uncomment the line below to connect to the databricks cluster
    # st.write(f"The session state type: {type(st.session_state['databricks_connection'])}")
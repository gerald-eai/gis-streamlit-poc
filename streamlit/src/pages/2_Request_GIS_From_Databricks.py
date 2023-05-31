import streamlit as st
from utils.init_app_session import app_setup_on_load, init_db_connection, init_state

app_setup_on_load()

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

st.markdown(""" <p>This page makes requests to the Databricks Cluster using the Databricks REST API to retrieve the GIS Data for the Mains and the Network Meter</p>""",unsafe_allow_html=True)
# st.text('This page makes requests to the Databricks Cluster using the Databricks REST API to retrieve the GIS Data for the Mains and the Network Meter')
st.divider()
vert_space = '<div style="padding: 20px 5px;"></div>'
st.markdown(vert_space, unsafe_allow_html=True)
main_container = st.container()
# st.markdown("# Still Under Development :construction:")
main_container.write("# Still Under Development :construction:")
st.markdown(vert_space, unsafe_allow_html=True)
st.divider()

# have a button that triggers a data request
# if st.button('Fetch Lower Hall B Layer'):
#     # start in a loading state
#     with st.spinner('Requesting Data...'):
#         lower_hall_gdf = await request_base_layer()
#         # display the data once retrieved
#         st.write(f"Data retrieved: {type(lower_hall_gdf)}")
#         st.dataframe(lower_hall_gdf)
#         # save_data_to_state(lower_hall_gdf, 'lower_hallb_layer')
#         # st.write(f"Data saved to state: \n{st.session_state['lower_hallb_layer'].head()}")
# st.divider()
# # display a rough version of the map
# fig, base = gen_base_layer()
# st_folium.st_folium(fig=base, width=700, height=700)


# # st.text("Requesting the Network Meter Data") 
# # # Perform state check
# if st.session_state['ntwk_meter_df'] is None:
#     if st.button("Request Network Meter Data"):
#         st.session_state['ntwk_meter_df'] = request_ntwk_meter_data(selected_fmz=fmz_list)
#         # get_ntwk_session(fmz_list)
#         st.dataframe(st.session_state['ntwk_meter_df'])
#         # st.session_state['ntwk_meter_df'] = await request_ntwk_meter_data(selected_fmz=fmz_list) # test with fmz_list 
#         st.success('FMZ DF Requested')
# else:    
#     st.write("Process the requested data!")
#     st.dataframe(st.session_state['ntwk_meter_df'])
#     # create a base map to view the data
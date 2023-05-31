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
import streamlit as st
from pipeline-1-etl import pipeline

st.title('Files Processing')

if st.button('Process Files'):
    with st.spinner('Processing...'):
        logs = pipeline()
        for log in logs:
            st.write(log)
    st.success('Done!')
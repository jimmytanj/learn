import streamlit as st
import pandas as pd
import numpy as np
# 29.35 106.33
data=pd.DataFrame(
    np.random.randn(1000, 2) / [50, 50] + [29.35, 106.33],
   columns=['lat', 'lon'])
st.map(data)
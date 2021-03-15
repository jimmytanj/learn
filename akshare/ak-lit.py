import streamlit as st
import akshare as ak
import altair as alt
my_fund=[{'code':'0001630','name':'天弘中证计算机主题ETF连接C'}]

res=ak.fund_em_open_fund_info(fund="001630", indicator="单位净值走势")

st.dataframe(res.iloc())
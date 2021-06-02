import json

import streamlit as st
from PIL import Image
import numpy as np
import time
import pandas as pd

from aip import AipOcr

APP_ID = '24261698'
API_KEY = 'bCelQGkmXjUqjMS2RyGXkXha'
SECRET_KEY = '6IdKDVSirtf0VOUpFwbN2RhLCGelyoHP'
img_path = r'C:\Users\tan_jianming\Downloads\page00729-3.jpg'
client = AipOcr(APP_ID, API_KEY, SECRET_KEY)
client.setConnectionTimeoutInMillis(3000)
client.setSocketTimeoutInMillis(6000)


def img2table(img_bytes) -> str():
    res = client.tableRecognitionAsync(img_bytes)
    res_json = json.loads(json.dumps(res))
    print(res_json)
    if "result" in res_json:
        print("请求识别成功。")
        request_id = res_json["result"][0]["request_id"]
        rec_res = client.getTableRecognitionResult(request_id)
        while(rec_res["result"]["ret_msg"]!="已完成"):
            time.sleep(1)
            rec_res = client.getTableRecognitionResult(request_id)
        if rec_res["result"]["result_data"]:
            result_data = rec_res["result"]["result_data"]
            print("识别结果返回成功，结果为:{}".format(result_data))
            return result_data
        else:
            return ''
    else:
        return ''
st.set_page_config(page_title="地县志-图片转表格")

uploaded_file = st.file_uploader("上传一张图片,只支持jpg", type="jpg")
if uploaded_file is not None:
    bytes_data = uploaded_file.read()
    image=Image.open(uploaded_file)
    img_array = np.array(image)
    if image is not None:
        st.image(
            image,
            caption=f"You amazing image has shape {img_array.shape[0:2]}",
            use_column_width=True,
        )
    st.write("识别中...")
    res=img2table(bytes_data)
    st.write("识别完毕...")
    df=pd.read_excel(res)
    st.dataframe(df)
    st.write("识别结果:{}".format(res))
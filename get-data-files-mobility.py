# -*- coding: utf-8 -*-
import os
import pandas as pd
import urllib
import ast
import numpy as np
from tqdm import tqdm

os.chdir("")

cities = pd.read_csv("index-cities.csv")

cidxs = [idx*100 for idx in cities.city_idx]

cities['city_idx'] = cidxs 

###move in data
moveins = []
for i in tqdm(range(len(cities))):
    idx = cities.prov_idx[i]*10000 #add extra zeros for url address
    name_ch = cities.prov_ch[i]
    name_en = cities.prov_en[i]
    url = "http://huiyan.baidu.com/migration/historycurve.jsonp?dt=province&id="+str(idx)+"&type=move_in"
    file = urllib.request.urlopen(url, timeout=20) #increase timeout to avoid connection error
    file = file.read()
    dict_str = file.decode("UTF-8")
    dict_str = dict_str.replace('\ncb({"errno":0,"errmsg":"SUCCESS","data":{"list":{', "{")
    dict_str = dict_str.replace("}})", "")
    data = ast.literal_eval(dict_str)
    data = pd.DataFrame.from_dict(data,orient='index')
    data.columns = ["mobility"]
    data["date"] = data.index
    data.reset_index(drop=True, inplace=True)
    data['prov_idx'] = np.repeat(idx, len(data))
    data['prov_ch'] = np.repeat(name_ch, len(data))
    data['prov_en'] = np.repeat(name_en, len(data))
    moveins.append(data)
data = pd.concat(moveins)
data.to_csv("data_movein.csv", index=False, encoding='utf_8_sig')

###move out data
moveins = []
for i in tqdm(range(len(cities))):
    idx = cities.prov_idx[i]*10000 #add extra zeros for url address
    name_ch = cities.prov_ch[i]
    name_en = cities.prov_en[i]
    url = "http://huiyan.baidu.com/migration/historycurve.jsonp?dt=province&id="+str(idx)+"&type=move_out"
    file = urllib.request.urlopen(url, timeout=20) #increase timeout to avoid connection error
    file = file.read()
    dict_str = file.decode("UTF-8")
    dict_str = dict_str.replace('\ncb({"errno":0,"errmsg":"SUCCESS","data":{"list":{', "{")
    dict_str = dict_str.replace("}})", "")
    data = ast.literal_eval(dict_str)
    data = pd.DataFrame.from_dict(data,orient='index')
    data.columns = ["mobility"]
    data["date"] = data.index
    data.reset_index(drop=True, inplace=True)
    data['prov_idx'] = np.repeat(idx, len(data))
    data['prov_ch'] = np.repeat(name_ch, len(data))
    data['prov_en'] = np.repeat(name_en, len(data))
    moveins.append(data)
data = pd.concat(moveins)
data.to_csv("data_moveout.csv", index=False, encoding='utf_8_sig')
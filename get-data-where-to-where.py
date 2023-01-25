# -*- coding: utf-8 -*-
import os
import pandas as pd
import urllib
import ast
import numpy as np
from tqdm import tqdm
import datetime


os.chdir("")

code_data = pd.read_csv("code_data.csv")

numdays = 3 #can be any number of days up to when Baidu has data
base = datetime.datetime.today()
dates_sep = [str(base - datetime.timedelta(days=x)).split(" ")[0] for x in range(numdays)]
dates = [str(base - datetime.timedelta(days=x)).split(" ")[0].replace("-","") for x in range(numdays)]


###move in data
error_code = []
error_date = []
moveins = []
for d in range(len(dates[1:])):  
    date = dates[1:][d]
    by_cities = []
    for i in tqdm(range(len(code_data))):
        code = code_data.City_Code[i]
        url = "http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id="+str(code)+"&type=move_in&date="+date
        try:
            file = urllib.request.urlopen(url, timeout=20) #increase timeout to avoid connection error
            file = file.read()
            dict_str = file.decode("UTF-8")
            dict_str = dict_str.replace('\ncb({"errno":0,"errmsg":"SUCCESS","data":{"list":[{', "{")
            dict_str = dict_str.replace("]}})", "")
            data = ast.literal_eval(dict_str)
            data = pd.DataFrame(list(data))
            data.columns = ["City_CH", "Prov_CH", "proportion_in"]
            data["City_EN"] = [code_data[code_data.City_CH==c].City_EN.values[0] for c in data.City_CH]
            data["Prov_EN"] = [code_data[code_data.City_CH==c].Prov_EN.values[0] for c in data.City_CH]
            data['Towards_City'] = np.repeat(code_data[code_data.City_Code==code].City_EN.values[0], len(data))
            data['Towards_Prov'] = np.repeat(code_data[code_data.City_Code==code].Prov_EN.values[0], len(data))
            by_cities.append(data)
        except:
            error_code.append(code)
            error_date.append(dates_sep[1:][d])
            continue
    errors = pd.DataFrame({"error_code":error_code, "error_date":error_date})
    data = pd.concat(by_cities)
    data['date'] = dates_sep[1:][d]
    moveins.append(data)
data = pd.concat(moveins)
data.to_csv("data_movein.csv", index=False, encoding='utf_8_sig')

###move out data
error_code = []
error_date = []
moveins = []
for d in range(len(dates[1:])):  
    date = dates[1:][d]
    by_cities = []
    for i in tqdm(range(len(code_data))):
        code = code_data.City_Code[i]
        url = "http://huiyan.baidu.com/migration/cityrank.jsonp?dt=province&id="+str(code)+"&type=move_out&date="+date
        try:
            file = urllib.request.urlopen(url, timeout=20) #increase timeout to avoid connection error
            file = file.read()
            dict_str = file.decode("UTF-8")
            dict_str = dict_str.replace('\ncb({"errno":0,"errmsg":"SUCCESS","data":{"list":[{', "{")
            dict_str = dict_str.replace("]}})", "")
            data = ast.literal_eval(dict_str)
            data = pd.DataFrame(list(data))
            data.columns = ["City_CH", "Prov_CH", "proportion_out"]
            data["City_EN"] = [code_data[code_data.City_CH==c].City_EN.values[0] for c in data.City_CH]
            data["Prov_EN"] = [code_data[code_data.City_CH==c].Prov_EN.values[0] for c in data.City_CH]
            data['From_City'] = np.repeat(code_data[code_data.City_Code==code].City_EN.values[0], len(data))
            data['From_Prov'] = np.repeat(code_data[code_data.City_Code==code].Prov_EN.values[0], len(data))
            by_cities.append(data)
        except:
            error_code.append(code)
            error_date.append(dates_sep[1:][d])
            continue
    errors = pd.DataFrame({"error_code":error_code, "error_date":error_date})
    data = pd.concat(by_cities)
    data['date'] = dates_sep[1:][d]
    moveins.append(data)
data = pd.concat(moveins)
data.to_csv("data_moveout.csv", index=False, encoding='utf_8_sig')
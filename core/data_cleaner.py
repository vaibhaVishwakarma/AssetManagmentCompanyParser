import yaml
import pandas as pd
import numpy as np
import re 
import os


f = open("./config/amc_configs2.yaml", "r")
configs = yaml.safe_load(f)
f.close()

output_folder = configs.get("Defaults",{}).get("OutputDirectory","./cleaned")
file_paths = list(os.walk(output_folder))[0][2]
file_paths = list(filter(lambda x : "transformed" not in x , file_paths))

def cleanString(s):
    s = str(s)
    s1 = floatFilter(s)
    if s1 :
        return s1[0]
    s2 = integerFilter(s[0])
    if s2 :
        return s2[0]
    return 0

# def dropCriteria(row):
#     return sum([1 if item else 0 for item in row.to_list()[:3]]) < 2


for filename in file_paths:
    df = pd.read_excel(f"{output_folder}/{filename}")
    floatFilter = lambda x : re.findall(r"-?\d+\.{1}\d+",x)
    integerFilter = lambda x : re.findall(r"-?\d+",x)
    amc_name = df["AMC"].unique()[0]
    config = configs.get(amc_name,{})
    scalebyhundred = config.get("Scale100",[])
    scalebyhundred = list(map(str.lower , scalebyhundred))
    factorbyhundred = config.get("Scale100th",[])   
    factorbyhundred = list(map(str.lower , factorbyhundred))

    numeric_columns = ["coupon" , "quantity" , "market value (mkt) ( rs lakh )" , "% to net assets (nav)" , "yield", "yield to call (ytc)"]

    for col in numeric_columns:
        if df[col].dtype == "object":
            df[col] = df[col].map(cleanString).map(np.float64)
        if col in factorbyhundred:
            df[col] = df[col].apply(lambda x: np.divide(x, 100))
        if col in scalebyhundred:
            df[col] = df[col].apply(lambda x : np.multiply(x, 100))
        df[col] = df[col].fillna(0)

    # df = df[df.apply(dropCriteria , axis = 1)]

    df.to_excel(f"final_cleaned/{filename}" , index = False)





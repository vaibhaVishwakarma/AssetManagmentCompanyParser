import os
import yaml
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
load_dotenv()

CONFIG_PATH = os.getenv("CONFIG_PATH")
ISIN_LOOKUP_PATH = r".\\ISIN\\fund_isin.xlsx"

# Import your actual parser classes
from core.amcparser import AMCPortfolioParser

# Load all configurations
def load_yaml_config(filepath=CONFIG_PATH):
    with open(filepath, "r") as f:
        config = yaml.safe_load(f)
    return config

def get_isin_map(ISIN_LOOKUP_PATH):
    df = pd.read_excel(ISIN_LOOKUP_PATH)
    isin_mapping = {}
    for index, row in df.iterrows():
        fund_name = row['Cleaned Fund Name'].lower()
        isin = row['ISIN']
        if fund_name and isin and row['Growth/Regular Type'] in ["Growth", "Regular"]:
            isin_mapping[fund_name] = isin
    return isin_mapping

def get_name_map(amc_name, fund_name):
    name_map = dict()
    for amc in amc_name:
        if name_map.get(amc) :
            continue
        for fund in fund_name:
            words = fund.split(" ")
            if words[0].lower() in amc.lower():
                name_map[amc] = fund
                break
    return name_map


if __name__ == "__main__":
    

    config_json = load_yaml_config()

    AMC_NAMES = list(config_json.keys())
    amc_name_value = [ x["amc_name"] for x in list(config_json.values())]
    isin_mapping = get_isin_map(ISIN_LOOKUP_PATH)
    fund_name = list(isin_mapping.keys())
    amc_to_fund_map = get_name_map(amc_name= amc_name_value, fund_name=fund_name)
    # print(fund_to_amc_map)
    for AMC in AMC_NAMES:
    # AMC = "dsp_mutual_fund"
        config = config_json[AMC]
        parser = AMCPortfolioParser(config)
        print(f"---- Processing {AMC} ---- ")
        try:
            parser.run()
            fund_name = amc_to_fund_map[config.get("amc_name")]
            # parser.append("main_output.xlsx", "fund_name"  ,  "isin_mapping[fund_name]" )
            parser.append("main_output.xlsx", fund_name  ,  isin_mapping[fund_name] )
        except Exception as e:
            print(e)    



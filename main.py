import os
import yaml
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

CONFIG_PATH = os.getenv("CONFIG_PATH")

# Import your actual parser classes
from core.amcparser import AMCPortfolioParser

# Load all configurations
def load_yaml_config(filepath=CONFIG_PATH):
    with open(filepath, "r") as f:
        config = yaml.safe_load(f)
    return config

if __name__ == "__main__":
    config_json = load_yaml_config()

    AMC_NAMES = list(config_json.keys())

    for AMC in AMC_NAMES:
    # AMC = "hdfc_mutual_fund"
        parser = AMCPortfolioParser(config_json[AMC])
        print(f"---- Processing {AMC} ---- ")
        try:
            parser.run()
        except Exception as e:
            print(e)
    

    
    

    



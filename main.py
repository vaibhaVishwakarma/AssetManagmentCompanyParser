import os
import yaml
import pandas as pd

# Import your actual parser classes
from core.parser import HDFCParser, ICICIMFParser, PPFASParser

# Load all configurations
def load_yaml_config(filepath="amc_configs.yaml"):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)

if __name__ == "__main__":
        # List of AMC names

    configs = load_yaml_config()

    amc_names = [
        "360 One Asset Management", "Aditya Birla Sun Life Mutual Fund", "Axis Mutual Fund", 
        "Bandhan Mutual Fund", "Bank of India Mutual Fund", "Baroda BNP Paribas Mutual Fund", 
        "Canara Robeco Mutual Fund", "DSP Mutual Fund", "Edelweiss Mutual Fund", "Franklin Templeton India", 
        "Groww Mutual Fund", "HDFC Mutual Fund", "Helios Mutual Fund", "HSBC Mutual Fund", 
        "ICICI Prudential Mutual Fund", "Invesco Mutual Fund", "ITI Mutual Fund", "JM Financial Mutual Fund", 
        "Kotak Mutual Fund", "LIC Mutual Fund", "Mahindra Manulife Mutual Fund", "Mirae Asset Mutual Fund", 
        "Motilal Oswal Mutual Fund", "Navi Mutual Fund", "Nippon India Mutual Fund", "NJ Mutual Fund", 
        "PGIM India Mutual Fund", "PPFAS Mutual Fund", "Quant Mutual Fund", "Quantum Mutual Fund", 
        "SBI Mutual Fund", "Shriram Mutual Fund", "Sundaram Mutual Fund", "Tata Mutual Fund", 
        "Trust Mutual Fund", "Union Mutual Fund", "UTI Mutual Fund", "WhiteOak Mutual Fund", 
        "Zerodha Fund House"
    ]

    for amc_name in amc_names:


        if amc_name == "PPFAS Mutual Fund":
            parser = PPFASParser(configs[amc_name])
        elif amc_name == "HDFC Mutual Fund":
            parser = HDFCParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel()
        elif amc_name == "ICICI Prudential Mutual Fund":
            parser = ICICIMFParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel()
        else:
            print(f"❌ No parser implemented for: {amc_name}")
            continue




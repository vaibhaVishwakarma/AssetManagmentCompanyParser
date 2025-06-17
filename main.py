import os
import yaml
import pandas as pd

# Import your actual parser classes
from core.parser import *


import sys
import atexit
import traceback

class DualLogger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.logfile = open(filename, 'w', encoding="UTF-8" , buffering=1)  # line-buffered
        atexit.register(self.cleanup)  # ensure file is closed at exit

    def write(self, message):
        self.terminal.write(message)
        self.logfile.write(message)

    def flush(self):
        self.terminal.flush()
        self.logfile.flush()

    def cleanup(self):
        self.logfile.close()

# Redirect stdout and stderr
sys.stdout = sys.stderr = DualLogger("output.txt")

# Optional: catch uncaught exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    print("\nUnhandled exception occurred:")
    traceback.print_exception(exc_type, exc_value, exc_traceback)

sys.excepthook = handle_exception


# Load all configurations
def load_yaml_config(filepath="config/amc_configs2.yaml"):
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
        "Zerodha Fund House",
    ]

    for amc_name in amc_names:

        print(f"🔍 Processing AMC: {amc_name}")

        if amc_name == "Kotak Mutual Fund":
            parser = KotakParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel() 
        # if amc_name == "Groww Mutual Fund":
        #     parser = GrowwParser(configs[amc_name])
        #     parser.parse_all_portfolios()
        #     parser.save_to_excel() 
        if amc_name == "PPFAS Mutual Fund":
            parser = PPFASParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel() 
        if amc_name == "Mirae Asset Mutual Fund":
            parser = MiraeAssetParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel() 
        if amc_name == "Quant Mutual Fund":
            parser = QuantParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel() 
        if amc_name == "SBI Mutual Fund":
            parser = SBIParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel() 
        if amc_name == "ICICI Prudential Mutual Fund":
            parser = ICICIMFParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel()    
        if amc_name == "HDFC Mutual Fund":
            print("Processing HDFC Mutual Fund")
            parser = HDFCParser(configs[amc_name])
            parser.parse_all_portfolios()
            parser.save_to_excel()




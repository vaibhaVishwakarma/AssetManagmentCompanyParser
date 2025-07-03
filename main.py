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
        self.logfile = open(filename, 'a+', encoding="UTF-8" , buffering=1)  # line-buffered
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
def load_yaml_config(filepath="config/amc_configs.yaml"):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)
    
if __name__ == "__main__":
        # List of AMC names

    configs = load_yaml_config()

    amc_parser_mapping = {
    # "360 One Asset Management": One360Parser,
    # "Aditya Birla Sun Life Mutual Fund": AdityaBirlaParser,
    # "Axis Mutual Fund": AxisParser,
    # "Bandhan Mutual Fund": BandhanParser,
    # "Bank of India Mutual Fund": BankOfIndiaParser,
    # "Baroda BNP Paribas Mutual Fund": BarodaBNPParser,
    # "Canara Robeco Mutual Fund": CanaraRobecoParser,
    # "DSP Mutual Fund": DSPParser,
    # "Edelweiss Mutual Fund": EdelweissParser,
    # "Franklin Templeton India": FranklinTempletonParser,
    # "Groww Mutual Fund": GrowwParser,
    # "HDFC Mutual Fund": HDFCParser,
    # "Helios Mutual Fund": HeliosParser,
    # "HSBC Mutual Fund": HSBCParser,
    # "ICICI Prudential Mutual Fund": ICICIMFParser,
    # "Invesco Mutual Fund": InvescoParser,
    # "ITI Mutual Fund": ITIParser,
    # "JM Financial Mutual Fund": JMFinancialParser,
    # "Kotak Mutual Fund": KotakParser,
    # "LIC Mutual Fund": LICParser,
    "Mahindra Manulife Mutual Fund": MahindraManulifeParser,
    "Mirae Asset Mutual Fund": MiraeAssetParser,
    "Motilal Oswal Mutual Fund": MotilalOswalParser,
    "Navi Mutual Fund": NaviParser,
    "Nippon India Mutual Fund": NipponIndiaParser,
    "NJ Mutual Fund": NJParser,
    "PGIM India Mutual Fund": PGIMIndiaParser,
    "PPFAS Mutual Fund": PPFASParser,
    "Quant Mutual Fund": QuantParser,
    "Quantum Mutual Fund": QuantumParser,
    "SBI Mutual Fund": SBIParser,
    "Shriram Mutual Fund": ShriramParser,
    "Sundaram Mutual Fund": SundaramParser,
    "Tata Mutual Fund": TataParser,
    "Trust Mutual Fund": TrustParser,
    "Union Mutual Fund": UnionParser,
    "UTI Mutual Fund": UTIParser,
    "WhiteOak Mutual Fund": WhiteOakParser,
    "Zerodha Fund House": ZerodhaParser,
    "Old Bridge Capital": OldBridgeCapital,
    }

    default_config = configs["Defaults"]


    for amc_name in amc_parser_mapping:

        print(f"🔍 Processing AMC: {amc_name}")

        parser = amc_parser_mapping[amc_name](configs[amc_name] , default_config=default_config)
        parser.parse_all_portfolios()
        parser.save_to_excel()




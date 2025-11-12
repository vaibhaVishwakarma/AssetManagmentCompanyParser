# main.py
import os
import yaml
import logging
from core.amcparser import AMCPortfolioParser
from core.parser import *
from core.postprocessor import PortfolioPostProcessor
from langchain_huggingface import HuggingFaceEmbeddings
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# ---------- logger setup ----------
logger = logging.getLogger(__name__)
logging.basicConfig(
        filename='parser.log',
        level=logging.DEBUG,  # or ERROR
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        encoding='utf-8'
    )
    
# ---------- config loader ----------
def load_yaml_config(filepath="config/amc_configs.yaml"):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)

# ---------- mapping of AMC parsers ----------

amc_parser_mapping = {
    "360 One Asset Management": One360Parser,
    "Aditya Birla Sun Life Mutual Fund": AdityaBirlaParser,
    "Axis Mutual Fund": AxisParser,
    "Bandhan Mutual Fund": BandhanParser,
    "Bank of India Mutual Fund": BankOfIndiaParser,
    "Baroda BNP Paribas Mutual Fund": BarodaBNPParser,
    "Canara Robeco Mutual Fund": CanaraRobecoParser,
    "DSP Mutual Fund": DSPParser,
    "Edelweiss Mutual Fund": EdelweissParser,
    "Franklin Templeton India": FranklinTempletonParser,
    "Groww Mutual Fund": GrowwParser,
    "HDFC Mutual Fund": HDFCParser,
    "Helios Mutual Fund": HeliosParser,
    "HSBC Mutual Fund": HSBCParser,
    "ICICI Prudential Mutual Fund": ICICIMFParser,
    "Invesco Mutual Fund": InvescoParser,
    "ITI Mutual Fund": ITIParser,
    "JM Financial Mutual Fund": JMFinancialParser,
    "Kotak Mutual Fund": KotakParser,
    "LIC Mutual Fund": LICParser,
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

# ---------- main runner ----------
if __name__ == "__main__":
    configs = load_yaml_config()
    default_config = configs.get("Defaults", {})
    embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2") 


    # --------------------------------------------------------------
    # 1. Cleanup intermediate folders (.cleaned, .final_cleaned)
    # --------------------------------------------------------------
    for folder in [".cleaned", ".final_cleaned"]:
        os.makedirs(folder, exist_ok=True)
        for name in os.listdir(folder):
            path = os.path.join(folder, name)
            try:
                if os.path.isfile(path) or os.path.islink(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    # recursively clear nested contents
                    for root, dirs, files in os.walk(path, topdown=False):
                        for f in files:
                            os.remove(os.path.join(root, f))
                        for d in dirs:
                            os.rmdir(os.path.join(root, d))
            except Exception as e:
                logger.warning(f"Cleanup failed for {path}: {e}")\
        

    # --------------------------------------------------------------
    # 2. Extract Raw data from Excels
    # --------------------------------------------------------------
    for amc_name, parser_cls in amc_parser_mapping.items():
        logger.info(f"=== Processing AMC: {amc_name} ===")
        try:
            amc_cfg = configs.get(amc_name, {})
            parser = parser_cls(amc_cfg, default_config, embedding_model)
            parser.parse_all_portfolios()
            parser.save_to_excel()
        except Exception as e:
            logger.exception(f"Error processing AMC {amc_name}: {e}")

    # --------------------------------------------------------------
    # 3. Post Processing
    # --------------------------------------------------------------
    try:
        post = PortfolioPostProcessor()
        post.clean_data()
        post.compile_final_output()
        logger.info("=== Pipeline completed successfully ===")
    except Exception as e:
        logger.exception(f"Post-processing failed: {e}")

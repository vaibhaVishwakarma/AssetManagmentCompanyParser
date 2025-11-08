# main.py
import os
import yaml
import logging
from core.amcparser import AMCPortfolioParser
from core.parser import *
from core.postprocessor import PortfolioPostProcessor

# ---------- logger setup ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ---------- config loader ----------
def load_yaml_config(filepath="config/amc_configs.yaml"):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)

# ---------- mapping of AMC parsers ----------

amc_parser_mapping = {
"360 One Asset Management": One360Parser,
}

# ---------- main runner ----------
if __name__ == "__main__":
    configs = load_yaml_config()
    default_config = configs.get("Defaults", {})

    for amc_name, parser_cls in amc_parser_mapping.items():
        logger.info(f"=== Processing AMC: {amc_name} ===")
        try:
            amc_cfg = configs.get(amc_name, {})
            parser = parser_cls(amc_cfg, default_config)
            parser.parse_all_portfolios()
            parser.save_to_excel()
        except Exception as e:
            logger.exception(f"Error processing AMC {amc_name}: {e}")

    # post-processing: clean and compile
    try:
        post = PortfolioPostProcessor()
        post.clean_data()
        post.compile_final_output()
        logger.info("=== Pipeline completed successfully ===")
    except Exception as e:
        logger.exception(f"Post-processing failed: {e}")

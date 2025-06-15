import re
import pandas as pd
import os
from abc import ABC, abstractmethod


class AMCPortfolioParser(ABC):
    def __init__(self, config):
        self.data_dir = config.get("data_dir")
        self.output_file = config.get("output_file", "parsed_portfolio.xlsx")
        self.amc_name = config.get("amc_name")
        self.sheets_to_avoid = config.get("sheets_to_avoid", [])
        self.column_mapping = config.get("column_mapping", {})
        self.final_columns = config.get("final_columns", []) 
        self.fund_name_extraction_logic = config.get("fund_name_extraction_logic", self._default_fund_name_extraction)
        self.instrument_type_logic = config.get("instrument_type_logic", self._default_instrument_type_logic)
        self.full_data = pd.DataFrame()
        self.isin_lookup =self._create_ISIN_mapping(pd.read_excel(config.get("ISIN_file"))) #TODO: Make this configurable or pass as an argument


    def _create_ISIN_mapping(df):
        
        """Create a mapping of fund names to ISINs."""
        isin_mapping = {}
        for index, row in df.iterrows():
            fund_name = row['Cleaned Fund Name'].lower()
            isin = row['ISIN']
            if fund_name and isin and row['Growth/Regular Type'] in ["Growth", "Regular"]:
                isin_mapping[fund_name] = isin
        return isin_mapping    

    def _default_fund_name_extraction(self, sheet_df):
        # Default logic for fund name extraction (similar to original script)
        amc_norm = self.amc_name.strip().lower()
        top6 = sheet_df.head(6).astype(str)
        candidates = []
        for cell in top6.values.flatten():
            txt = cell.strip()
            low = txt.lower()
            if "fund" in low and low != amc_norm:
                candidates.append(txt)
        if not candidates:
            return None
        raw = max(candidates, key=len)
        name = re.sub(r"\s+", " ", raw).strip()
        name = re.sub(r"[—–\-\:\;,\s]+$", "", name)
        return name

    def _default_instrument_type_logic(self, df_clean):
        # Default logic for instrument type determination
        df_clean[["Yield"]] = df_clean[["Yield"]].fillna(value=0)
        df_clean["Type"] = df_clean["Yield"].apply(lambda x: "Debt or related" if x != 0 else "Equity or Equity related")
        return df_clean

    def get_file_names(self):
        file_names = []
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith((".xlsb", ".xls", ".xlsx")):
                    file_names.append(os.path.join(root, file))
        return file_names

    def read_excel_file(self, file_path):
        file_ext = file_path.split(".")[-1].lower()
        if file_ext == "xlsb":
            try:
                return pd.read_excel(file_path, sheet_name=None, engine="pyxlsb", dtype=str)
            except Exception as e:
                print(f"❌ Error reading {file_path} (XLSB format): {e}")
                return None
        elif file_ext in ["xls", "xlsx"]:
            try:
                return pd.read_excel(file_path, sheet_name=None, dtype=str)
            except Exception as e:
                print(f"❌ Error reading {file_path} (XLS/XLSX format): {e}")
                return None
        else:
            print(f"⚠️ Unsupported file format: {file_path}")
            return None

    @abstractmethod
    def process_sheet(self, datafile, sheet_name, sheet_df):    
        """
        Process each sheet in the Excel file.
        This method should be implemented in subclasses to handle specific parsing logic.
        """
        pass


    def parse_all_portfolios(self):
        filenames = self.get_file_names()
        for datafile in filenames:
            df_raw = self.read_excel_file(datafile)
            if df_raw is None:
                continue
            for sheet_name, sheet_df in df_raw.items():
                if sheet_name not in self.sheets_to_avoid:
                    self.process_sheet(datafile, sheet_name, sheet_df)

    def save_to_excel(self):
        if not self.full_data.empty:
            self.full_data.to_excel(self.output_file, index=False)
            print(f"✅ Successfully saved parsed data to {self.output_file}")
        else:
            print("⚠️ No data to save.")


# Example Usage (for ICICI Prudential Mutual Fund)
if __name__ == "__main__":
    icici_config = {
        "data_dir": "/home/ubuntu/upload/", # Assuming the Excel file is in the upload directory
        "output_file": "icici_mutual_fund_portfolio_parsed.xlsx",
        "amc_name": "ICICI Prudential Mutual Fund",
        "sheets_to_avoid": [],
        "column_mapping": {},
        "final_columns": [
            "Name of Instrument", "ISIN", "Coupon", "Industry", "Quantity", 
            "Market Value", "% to Net Assets", "Yield", "Yield to call", 
            "Type", "Scheme Name", "AMC"
        ]
    }

    parser = AMCPortfolioParser(icici_config)
    parser.parse_all_portfolios()
    parser.save_to_excel()

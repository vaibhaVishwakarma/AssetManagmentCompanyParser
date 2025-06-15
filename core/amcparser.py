import re
import pandas as pd
import os
from abc import ABC, abstractmethod


#--additional--
import requests
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
OUTPUT_FOLDER = "./output"

#--trivial warning--
# import warnings
# warnings.simplefilter(action='ignore', category=FutureWarning)


class AMCPortfolioParser:
    def __init__(self, config):
        self.data_dir = config.get("data_dir")
        self.output_file = config.get("output_file", "parsed_portfolio.xlsx")
        self.amc_name = config.get("amc_name")
        self.sheets_to_avoid = config.get("sheets_to_avoid", [])
        self.column_mapping = config.get("column_mapping", {})
        self.final_columns = config.get("final_columns", []) 
        self.fund_name_extraction_logic = config.get("fund_name_extraction_logic", self._default_fund_name_extraction)
        self.instrument_type_logic = config.get("instrument_type_logic", self._default_instrument_type_logic)
        # --additional--
        self.base_headers = ["Name of Instrument","ISIN" , "Industry" , "Yield" , "Quantity" , "Market Value" , "Net Asset Value (NAV)"]
        self.full_data = pd.DataFrame(columns= self.base_headers + ["Type" , "Scheme" , "AmcName"])
        self.base_embeddings = np.array([self._generate_embedding(value) for value in self.base_headers])

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
        print(f"\n🔍 Processing  → Sheet: {sheet_name}")

        fund = self.fund_name_extraction_logic(sheet_df)

        if fund is None:
            print(f"⚠️ Skipping {sheet_name} (Could not extract fund name)")
            return

        print(f"\n🔍 Processing  → Fund: {fund}")
        header_row_idx = next(
            (index for index, row in sheet_df.iterrows() if any("ISIN" in str(val) for val in row.dropna())),
            None
        )
        if header_row_idx is None:
            print(f"⚠️ Skipping {sheet_name} (No ISIN header found)")
            return

        header_row_idx = next(
            (index for index, row in sheet_df.iterrows() if any("ISIN" in str(val) for val in row.dropna())),
            None
        )
        if header_row_idx is None:
            print(f"⚠️ Skipping {sheet_name} (No ISIN header found)")
            return

        df = pd.read_excel(datafile, sheet_name=sheet_name, skiprows=header_row_idx, dtype=str)
        df = df.dropna(how='all')

        #find and map base_headers with varying headers of the data
        header_row = self._fetch_header_row(df)
        print(header_row)
        header_map = self._header_mapper(header_row)
        print("header_map....",header_map)

        
        threshold = 0.6
        # Get row indices where the proportion of non-null entries is less than 60%
        start_indexes = df.index[df.notna().mean(axis=1) < threshold].tolist()

        print("start indexes...",start_indexes)

        #define investment types along with data garbage
        rows = df.fillna("").astype(str).agg(' '.join, axis=1)
        investment_types = [rows[investment_type_idx].strip() for investment_type_idx in start_indexes]

        #process data piece by piece
        for i in range(len(start_indexes)):
            #[]----modification needed----
            investment_type = investment_types[i]

            start_index = start_indexes[i]
            if i != len(start_indexes)-1 : end_index = start_indexes[i+1]
            else : end_index = len(df)
            print(f"working on index : {start_index} , investment type  {investment_type} ")
            
            for (index,row) in df.iloc[start_index:end_index].iterrows():
                values = header_map.copy()
                print("here:",row)
                for (key , idx) in header_map.items():
                    values[key] = row[idx]
                isin = values['ISIN']
                # print("here:",isin)
                if not str(isin).lower().startswith("in"): # skip if isin is invalid
                    continue
                print(f"{index} ",end=" , ") # just to keep track

                #meta data addition
                values["Type"] =  investment_type
                values["Scheme"] = sheet_name
                values["AmcName"] = self.amc_name           

                self.full_data = pd.concat([self.full_data , pd.DataFrame([values])],ignore_index=True).drop_duplicates()
                print(values)
        print("sheet over")
        
    
    # ------------ functions added by vaibhav  ---------------
    def _generate_embedding(self, text:str) -> list[float]:
        url = "https://lamhieu-lightweight-embeddings.hf.space/v1/embeddings"
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        data = {
            "model": "snowflake-arctic-embed-l-v2.0",
            "input": text
        }

        response = requests.post(url, headers=headers, json=data)
        if response.ok:
            return response.json()["data"][0]["embedding"]
        else:
            raise Exception("No response")

    def _fetch_header_row(self, df :pd.DataFrame) -> list[str]: 
        rows = df.astype(str).agg(' '.join, axis=1)
        idx = rows[rows.apply(lambda x: "instrument" in x.lower())].index.tolist()[0]
        header_row = df.iloc[idx,:].fillna("NULL")
        header_row = [(header_row.iloc[col]) for col in range(header_row.shape[0])]
        return header_row
    

    
    def _header_mapper(self, header_row ) -> {str:int}:

        header_map = dict()

        header_row_embeddings = np.array([self._generate_embedding(value) for value in header_row])
        # Compute cosine similarity (shape: 5 x 10)
        similarity_matrix = cosine_similarity(self.base_embeddings, header_row_embeddings)

        # For each base vector, find the index of the most similar header
        most_similar_indices = np.argmax(similarity_matrix, axis=1)

        # Optionally, get the similarity score too
        most_similar_scores = np.max(similarity_matrix, axis=1)

        # Print results
        for i, (idx, score) in enumerate(zip(most_similar_indices, most_similar_scores)):
            print(f"Base vector {i} ie {self.base_headers[i]} is most similar to header {idx} ie {header_row[idx]} with score {score:.4f}")
            header_map[self.base_headers[i]] = int(idx)
            
        return header_map
        

        # # Read the Excel file, skipping rows up to the header, and explicitly setting header=None
        # df_clean = pd.read_excel(datafile, sheet_name=sheet_name, skiprows=header_row_idx, header=None, dtype=str)

        # # Set the first row of the new DataFrame as the header
        # df_clean.columns = df_clean.iloc[0]

        # # Remove the header row from the data
        # df_clean = df_clean[1:].reset_index(drop=True)

        # df_clean = df_clean.loc[:, df_clean.columns.notna()] # Remove unnamed columns

        # print("Columns after initial read and unnamed column removal:", df_clean.columns.tolist()) # Debug print

        # # Apply column mapping
        # df_clean.rename(columns=self.column_mapping, inplace=True)

        # print("Columns after renaming:", df_clean.columns.tolist()) # Debug print

        # # Ensure 'Coupon' column exists and is numeric
        # if "Coupon" not in df_clean.columns:
        #     df_clean["Coupon"] = 0
        # else:
        #     df_clean["Coupon"] = pd.to_numeric(df_clean["Coupon"], errors='coerce').fillna(0)

        # # Filter out rows with missing essential data
        # df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)

        # # Apply instrument type logic
        # df_clean = self.instrument_type_logic(df_clean)

        # df_clean = df_clean.round(2)
        # df_clean["Scheme Name"] = fund
        # df_clean["AMC"] = self.amc_name

        # # Reorder and select final columns if specified
        # if self.final_columns:
        #     # Ensure all final_columns are present, fill missing with NaN
        #     for col in self.final_columns:
        #         if col not in df_clean.columns:
        #             df_clean[col] = None
        #     df_clean = df_clean[self.final_columns]

        # self.full_data = pd.concat([self.full_data, df_clean], ignore_index=True) if not self.full_data.empty else df_clean

    def parse_all_portfolios(self):
        filenames = self.get_file_names()
        n_iter = 0
        for datafile in filenames:
            df_raw = self.read_excel_file(datafile)
            if df_raw is None:
                continue
            for sheet_name, sheet_df in df_raw.items():
                if sheet_name not in self.sheets_to_avoid:
                    if n_iter > 10000:
                        break
                    self.process_sheet(datafile, sheet_name, sheet_df)
                    n_iter+=1

    def save_to_excel(self):
        if not self.full_data.empty:
            self.full_data.to_excel(self.output_file, index=False)
            print(f"✅ Successfully saved parsed data to {self.output_file}")
        else:
            print("⚠️ No data to save.")


# Example Usage (for ICICI Prudential Mutual Fund)
if __name__ == "__main__":

    icici_config = {
        "data_dir": r"data\\data\\Bank of India Mutual Fund\\",
        "output_file": f"{OUTPUT_FOLDER}/Bacnk_of_India_mutual_fund_portfolio_parsed.xlsx",
        "amc_name": "Bank of India Mutual Fund",
        "sheets_to_avoid": [],
        "column_mapping": {
            # Add any specific renames from the raw Excel headers to the desired final column names
            # For example, if the raw header is 'Name of the Instrument' but you want 'Name of Instrument'
            # Based on inspection, the raw headers seem to be mostly clean, but keeping this for flexibility
        },
        "final_columns": [
            "Name of Instrument", "ISIN", "Coupon", "Industry", "Quantity", 
            "Market Value", "% to Net Assets", "Yield", "Yield to call", 
            "Type", "Scheme Name", "AMC"
        ]
    }

    parser = AMCPortfolioParser(icici_config)
    parser.parse_all_portfolios()
    parser.save_to_excel()



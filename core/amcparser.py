import re
import pandas as pd
import os

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
        self.base_headers = ["Name of Instrument","ISIN" , "Rating/Industry" , "Yield" , "Quantity" , "Market Value" , "Net Asset Value (NAV)"]
        self.full_data = pd.DataFrame(columns= self.base_headers + ["Type" , "Scheme" , "AmcName"])
        self.base_embeddings = np.array([self._generate_embedding(value) for value in self.base_headers])

    def _default_fund_name_extraction(self, sheet_name, sheet_df):
        # Default logic for fund name extraction (similar to original script)
        amc_norm = self.amc_name.strip().lower()
        top6 = sheet_df.head(6).astype(str)
        candidates = [sheet_name]
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
    
    def create_ISIN_mapping(self , df):
        """Create a mapping of fund names to ISINs."""
        
        isin_mapping = {}
        for index, row in df.iterrows():
            fund_name = row['Cleaned Fund Name'].lower()
            isin = row['ISIN']
            if fund_name and isin and row['Growth/Regular Type'] in ["Growth", "Regular"]:
                isin_mapping[fund_name] = isin
        return isin_mapping


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
        try:  
            if file_ext == "csv" :
                return pd.read_csv(file_path, sheet_name=None, dtype=str)
            elif file_ext in ["xls", "xlsx"]:
                    return pd.read_excel(file_path, sheet_name=None, dtype=str)
        except Exception as e:
            print(f"⚠️ Unsupported file format:{file_ext} \n Path: {file_path}")

        return None
    
    def process_sheet(self, datafile, sheet_name, sheet_df):
        print(f"\n🔍 Processing  → Sheet: {sheet_name}")

        fund = self.fund_name_extraction_logic(sheet_name, sheet_df)

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

        if "csv" not in sheet_name:
            df = pd.read_excel(datafile, sheet_name=sheet_name, skiprows=header_row_idx, dtype=str)
        else:
            df = pd.read_csv(datafile, sheet_name=sheet_name, skiprows=header_row_idx, dtype=str)
        df = df.dropna(how='all')
        df.reset_index(drop=True , inplace = True)
        rows = df.fillna(" ").agg(" ".join , axis = 1)
        df = df.iloc[rows[rows.apply(lambda x : "listing on stock exchange" not in x.lower())].index.to_list()]
        df.reset_index(drop=True , inplace = True)
        
        #find the row contaning headers
        header_row = self._fetch_header_row(df)

        #clear any columns with null values and merge
        n_iter = 0
        while "NULL" in header_row and n_iter<5:
            start = None
            end = len(header_row)
            for i in range(len(header_row)):
                if start == None and header_row[i] != "NULL":
                    start = i
                    break
            for i in range(start+1 , len(header_row)):
                if header_row[i] != "NULL":
                    end = i
                    break
            alter1 = df.iloc[:,start:end].fillna("").agg(" ".join,axis = 1)
            alter2 = df.drop(df.columns[start:end],axis = 1)
            df = pd.concat([alter1 , alter2] , axis = 1)

            header_row = self._fetch_header_row(df)
            n_iter+=1

        #maps the desired columns 
        header_map = self._header_mapper(header_row)
        print("header_map....",header_map)

        periods = self._get_valid_periods(df , header_map)

        #process data piece by piece
        for (start_idx , end_idx) in periods:
            scheme_name = re.sub("[^a-zA-Z0-9\s]" , "" , df[start_idx-1:start_idx].fillna("").agg(" ".join , axis = 1).iloc[0])
            print("\n",scheme_name)

            for (index , row) in df.iloc[start_idx:end_idx+1].iterrows():
                values = header_map.copy()
                for (key , idx) in header_map.items():
                    values[key] = row.iloc[idx]
                print(f"{index} ",end=" , ") # just to keep track

                #meta data addition
                values["Type"] =  scheme_name
                values["Scheme"] = sheet_name
                values["AmcName"] = self.amc_name           

                self.full_data = pd.concat([self.full_data , pd.DataFrame([values])],ignore_index=True).drop_duplicates()
        print("sheet over")
        
    
    # ------------ functions added by vaibhav  ---------------
    def _check_isin(self, val):
        s = str(val).lower().strip()
        s = re.sub("[^a-zA-Z0-9]" , "" , s)
        return s.startswith("in") and s[-1] in "0123456789"
    
    def _get_valid_periods(self, df , header_map):
        mask = df.iloc[:, header_map["ISIN"]].apply(self._check_isin).values
        # Find continuous True periods
        periods = []
        start = None
        for i, val in enumerate(mask):
            if val:
                if start is None:
                    start = i
            else:
                if start is not None:
                    periods.append((start, i - 1))
                    start = None
        # Edge case: last element was True
        if start is not None:
            periods.append((start, len(mask) - 1))
        print("Passing periods:", periods)
        return periods
    
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

    def parse_all_portfolios(self):
        filenames = self.get_file_names()
        n_iter = 0
        for datafile in filenames:
            df_raw = self.read_excel_file(datafile)
            if df_raw is None:
                continue
            for sheet_name, sheet_df in df_raw.items():
                print("-"*30, sheet_name)
                if sheet_name not in self.sheets_to_avoid:
                    if n_iter > 1: # ---- to remove ----
                        break
                    self.process_sheet(datafile, sheet_name, sheet_df)
                    n_iter+=1

    def save_to_excel(self):
        if not self.full_data.empty:
            self.full_data.to_excel(self.output_file, index=False)
            print(f"✅ Successfully saved parsed data to {self.output_file}")
        else:
            print("⚠️ No data to save.")
    
    def run(self):
        self.parse_all_portfolios()
        self.save_to_excel()

    


# Example Usage (for ICICI Prudential Mutual Fund)
if __name__ == "__main__":

    icici_config = {
        "data_dir": r"data\\data\\Groww Mutual Fund",
        "output_file": f"{OUTPUT_FOLDER}/Groww_mutual_fund_portfolio_parsed.xlsx",
        "amc_name": "Groww Mutual Fund",
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



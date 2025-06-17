import re
import pandas as pd
import os
from abc import ABC, abstractmethod

import requests
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords

class AMCPortfolioParser(ABC):
    def __init__(self, config):
        self.data_dir = config.get("data_dir",None)
        self.output_file = config.get("output_file", "parsed_portfolio.xlsx")
        self.amc_name = config.get("amc_name",None)
        self.sheets_to_avoid = config.get("sheets_to_avoid", [])
        self.column_mapping = config.get("column_mapping", {})
        self.fund_name_extraction_logic = config.get("fund_name_extraction_logic", self._default_fund_name_extraction)
        self.instrument_type_logic = config.get("instrument_type_logic", self._default_instrument_type_logic)
        self.isin_lookup =self._create_ISIN_mapping(pd.read_excel(config.get("ISIN_file"))) #TODO: Make this configurable or pass as an argument

        self.final_columns = config.get("final_columns", None)
        if self.final_columns is None or len(self.final_columns) == 0:
            self.final_columns = [ "Name of Instrument", "ISIN", "Coupon", "Industry", "Quantity", "Market Value", "% to Net Assets (nav)",
    "Yield", "Yield to call (ytc)"]
            
        self.base_headers = [self._pre_process_header(header)for header in self.final_columns]
        self.full_data = pd.DataFrame(columns= self.base_headers+ ["Type", "Scheme Name", "AMC", "Scheme ISIN"])
        self.base_embeddings = np.array([self._generate_embedding(value) for value in self.base_headers])

        self.stopwords = set(stopwords.words("english"))

            

    def _get_fund_isin(self, fund_name):
        
        removestopwords = lambda x : " ".join([word for word in x.lower().split(" ") if word not in self.stopwords])
        transform = lambda x : re.sub(r"\([^\)]\)","",re.sub(r"[^a-zA-z0-9]","",x).lower())

        fund_name = removestopwords(fund_name)
        fund_name = transform(fund_name).split("fund")[0]

        fund_names = pd.Series(self.isin_lookup.keys()).astype(str)

        mask = fund_names.apply(lambda x : fund_name in transform(removestopwords(x)))
        candidate_fund_names = fund_names[mask].to_list()

        if len(candidate_fund_names):
            return self.isin_lookup.get(candidate_fund_names[0])
        else:
            return None

        # if fund_name.lower() in self.isin_lookup:
        #         print(f"ISIN for {fund_name.lower()}: {self.isin_lookup[fund_name.lower()]}")
        #         return self.isin_lookup[fund_name.lower()]
        # else:
        #      print(f"No ISIN found for {fund_name}")
        # return None

    
    def _create_ISIN_mapping(self,df):
        
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

    def process_sheet(self, datafile, sheet_name, sheet_df):    
        """
        Process each sheet in the Excel file.
        This method should be implemented in subclasses to handle specific parsing logic.
        """
        print(f"\n🔍 Processing  → Sheet: {sheet_name}")
        fund_name = self._get_fund_name(sheet_df)
        

        if fund_name is not None and sheet_name:
            fund_isin = self._get_fund_isin(fund_name)
            print(f"\n🔍 Processing  → Sheet: {fund_name}, {fund_isin}")

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
            rows = df.fillna(" ").agg(" ".join , axis = 1).apply(str.lower)
            df = df.iloc[rows[rows.apply(lambda x : "stock exchang" not in x and not ("index" in x and "stock" in x))].index.to_list()]
            df.reset_index(drop=True , inplace = True)
            table_end_idx = min(rows[rows.apply(lambda x : "grand total" in x)].index.to_list()[0]-1, len(df))
            
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
            # print("header_map....",header_map)
            

            # transform numerical columns to standard scale
            for header in self.base_headers[6:]:
                if header not in header_map.keys() : continue
                col = header_map[header]
                col_name = header_row[col]
                factor = None
                if "%" in header and "%" not in col_name:
                    factor = 100
                if "%" not in header and "%" in col_name:
                    factor = 0.01
                # if converstion needed.
                # if factor:
                #     try:
                #         df.iloc[:,col] = df.iloc[:,col].astype(str).apply(self._trf).astype(np.float32) * factor
                #         print(f"{col_name} Column Transformed to standard Units")
                #     except Exception as e:
                #         print("failed" , col_name)



            periods = self._get_valid_periods(df , header_map)

            #process data piece by piece
            for (start_idx , end_idx) in periods:

                type_name_idx = self._get_investment_type(df , start_idx , header_map["isin"])
                if(type_name_idx == start_idx):
                    print("No valid Type Name Found. Moving on.")
                    continue
                
                type_name =  df[type_name_idx:type_name_idx+1].fillna("").agg("".join , axis = 1).iloc[0]
                if("(" not in type_name): type_name = re.sub(r"[^\)]\)","",type_name)
                type_name = re.sub(r"\([^)]*\)", "", type_name)
                type_name = re.sub(r"[^a-zA-Z\s\&]" , "" , type_name)
                type_name = re.sub(r"(?<!\w)(nan)+(?!\w)", "", type_name ,flags=re.IGNORECASE)
                # print("\n",type_name)

                for (index , row) in df.iloc[start_idx:min(end_idx+1,table_end_idx)].iterrows():
                    values = header_map.copy()
                    for (key , idx) in header_map.items():
                        temp = row.iloc[idx]
                        values[key] = temp
                        # values[key] = temp if type(temp) not in ["str","object"] else  re.sub(r"[^a-zA-Z0-9.\s/\\]","",temp)
                    # print(f"{index} ",end=" , ") # just to keep track

                    #meta data addition
                    values["Type"] =  type_name
                    values["Scheme Name"] = fund_name
                    values["AMC"] = self.amc_name  
                    values["Scheme ISIN"] = fund_isin if fund_isin is not None else None         

                    self.full_data = pd.concat([self.full_data , pd.DataFrame([values])],ignore_index=True).drop_duplicates()
            print("sheet over")



    # --------OLD Functiones--------

    def _get_investment_type(self, df , start_index, isin_col_num):
        n_iter = 1 # to avoid endless loop
        while(n_iter<=10):
            candidate_isin = df.iloc[start_index-n_iter , isin_col_num]
            print(candidate_isin)
            if not self._check_isin(str(candidate_isin)):
                return start_index-n_iter
            n_iter+=1
        return start_index



    def _pre_process_header (self, x) :
        return re.sub(r"[^a-z\s%\(\)\\]","",x.lower())
    
    def _clean_fund_name(self,name):
        return re.sub(r"[^a-zA-z0.9\+\-\\\(\)\s/]","",name)
    
    def _trf(self, x):
        x = re.search(r'\d+(?:\.\d+)?', x)
        if x : x = x.group().strip()
        return "0" if x == None or x == "" else x

    def _filter_isin(self, string):
        s = str(string).lower().strip()
        return re.sub("[^a-zA-Z0-9]" , "" , s).upper()
    
    def _check_isin(self, val):
        return  (" " not in val and 
                    (val[0].isupper() and 
                    val[1].isupper() and 
                    (val[-1].isdigit() or val[-1].upper() == "X")))
                
    
    def _get_valid_periods(self, df , header_map):
        df.iloc[:, header_map["isin"]] = df.iloc[:, header_map["isin"]].apply(self._filter_isin)
        mask = df.iloc[:, header_map["isin"]].apply(self._check_isin).values
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
        # print("Passing periods:", periods)
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
    def _pre_process_header (self, x) :
        return re.sub(r"[^a-z\s%\(\)\\/]","",x.lower())
    
    def _header_mapper(self, header_row ) -> {str:int}:

        header_map = dict()

        self.base_headers = [self._pre_process_header(header) for header in self.base_headers]
        header_row = [self._pre_process_header(header) for header in header_row]

        header_row_embeddings = np.array([self._generate_embedding(value) for value in header_row])
        # Compute cosine similarity (shape: 5 x 10)
        similarity_matrix = cosine_similarity(self.base_embeddings, header_row_embeddings)

        # For each base vector, find the index of the most similar header
        most_similar_indices = np.argmax(similarity_matrix, axis=1)

        # Optionally, get the similarity score too
        most_similar_scores = np.max(similarity_matrix, axis=1)

        # Print results
        for i, (idx, score) in enumerate(zip(most_similar_indices, most_similar_scores)):
            bh = self.base_headers[i] 
            hr = header_row[idx]

            if "yield" in bh and "yield" not in hr:
                score = 0
            if bh == "yield":
                if "ytc" in hr or "call" in hr or "ytm" in hr:
                    score = 0
            if ("ytc" in bh or "call" in bh):
                if("ytc" in hr or "call" in hr) and "put" not in hr: 
                    score = 1
                else: 
                    score = 0 
            if ("ytm" in bh or "maturity" in bh): 
                if ("ytm" in hr or "maturity" in hr): 
                    score = 1
                else: 
                    score =0

            print(f"Base vector {i} ie {self.base_headers[i]} is most similar to header {idx} ie {header_row[idx]} with score {score:.4f}")
            if score > 0.51 :
                header_map[self.base_headers[i]] = int(idx)
        
            
        return header_map
    # --------OLD Functiones over--------


    def parse_all_portfolios(self):
        filenames = self.get_file_names()
        n_files = 10
        for datafile in filenames:
            df_raw = self.read_excel_file(datafile)
            if df_raw is None:
                continue
            for sheet_name, sheet_df in df_raw.items():
                if n_files <=0:
                    break
                if sheet_name not in self.sheets_to_avoid:
                    self.process_sheet(datafile, sheet_name, sheet_df)
                    n_files-=1

    def save_to_excel(self):
        error_saving = True
        while(error_saving):
            try:
                if not self.full_data.empty:
                    self.full_data.to_excel(self.output_file, index=False)
                    print(f"✅ Successfully saved parsed data to {self.output_file}")
                else:
                    print("⚠️ No data to save.")
                error_saving = False
            except Exception as e:
                continue


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

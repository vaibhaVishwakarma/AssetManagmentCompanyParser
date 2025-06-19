import re
import pandas as pd
import os
import sys
import time
from abc import ABC, abstractmethod

import requests
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords

class AMCPortfolioParser(ABC):

    def __init__(self, amc_config , default_config ):


        #independent variables
        self.amc_name = amc_config.get("AMCName",None)
        self.data_dir = amc_config.get("DataDirectory",f"./data/data/{self.amc_name}")
        self.output_directory = default_config.get("OutputDirectory","./")
        self.sheets_to_avoid = amc_config.get("sheets_to_avoid", [])
        self.isin_lookup = self._create_ISIN_mapping(pd.read_excel(default_config.get("ISINFilePath","./ISIN/fundisin.xlsx")))
        self.final_columns = amc_config.get("final_columns", None)

        #handle dervived variables
        self.output_file = f"{self.output_directory}/{self.amc_name}.xlsx"
        if self.final_columns is None or len(self.final_columns) == 0:
            self.final_columns = [  "Name of Instrument", "ISIN", "Coupon", "Industry", "Quantity", "Market Value", "% to Net Assets (nav)",
                                    "Yield", "Yield to call (ytc)" ,"Yield to Maturity (ytm)"]
            
        self.base_headers = [self._pre_process_header(header) for header in self.final_columns]
        self.full_data = pd.DataFrame(columns= self.base_headers + ["Type", "Scheme Name", "AMC", "Scheme ISIN"])
        self.base_embeddings = np.array([self._generate_embedding(value) for value in self.base_headers])

        self.stopwords = set(stopwords.words("english"))

        #functions
        self.filterNonAlphaNumeric = lambda x : re.sub(r"[^a-zA-z0-9]","",x)
        self.filterStopWords = lambda x : " ".join([word for word in str(x).lower().split(" ") if word not in self.stopwords])    
        self.filterBullets = lambda x :  x if "(" in x else re.sub(r"[^\)]\)", "", x)
        self.filterBracketContent = lambda x: re.sub(r"\([^\)]\)", "", x)
        self.filterNANIsolated = lambda x : re.sub(r"(?<!\w)(nan)+(?!\w)", "", x ,flags=re.IGNORECASE)
        self.filterReccuringSpaces = lambda x : re.sub(r"\s+"," ",x)

    def _get_fund_isin(self, fund_name):

        fund_name = self.filterStopWords(fund_name)
        #as per pattern after the word "fund" dates or irrelevent spefications are present
        fund_name = self.filterNonAlphaNumeric(fund_name).split("fund")[0].lower()

        fund_names = pd.Series(self.isin_lookup.keys()).astype(str).apply(str.lower)

        mask = fund_names.apply(lambda x : fund_name in self.filterNonAlphaNumeric(self.filterStopWords(x)))
        candidate_fund_names = fund_names[mask].to_list()

        if not candidate_fund_names:
            return None
        
        return self.isin_lookup.get(candidate_fund_names[0])

    def _create_ISIN_mapping(self,df):
        
        """Create a mapping of fund names to ISINs."""
        isin_mapping = {}
        for index, row in df.iterrows():
            fund_name = row['Cleaned Fund Name'].lower()
            isin = row['ISIN']
            if fund_name and isin and row['Growth/Regular Type'] in ["Growth", "Regular"]:
                isin_mapping[fund_name] = isin
        return isin_mapping    

    def _get_file_names(self):
        file_names = []
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith((".xlsb", ".xls", ".xlsx")):
                    file_names.append(os.path.join(root, file))
        return file_names

    def _read_excel_file(self, file_path , * , sheet_name = None, header_row_idx = None):

        file_ext = file_path.split(".")[-1].lower()
        try:
            if file_ext == "xlsb":
                return pd.read_excel(file_path, sheet_name=None, engine="pyxlsb", dtype=str)
            elif file_ext in ["xls", "xlsx"]:
                return pd.read_excel(file_path, sheet_name=None, dtype=str)
            elif file_ext == "csv":
                return pd.read_excel(file_path, sheet_name=sheet_name, skiprows=header_row_idx, dtype=str)
            
        except Exception as e:
            pass

        finally:
            print(f"⚠️ Error Reading file: {file_path}\n Supported File types xlsb/xls/xlsx")

        return None

    def process_sheet(self, file_path, sheet_name, df): #parsing logic    
        print(f"\n🔍 Processing  → Sheet: {sheet_name}")
        fund_name = self._get_fund_name(df)
        
        if not fund_name : print(f"No fund name for sheet {sheet_name}")
        if fund_name is not None and sheet_name:
            fund_isin = self._get_fund_isin(fund_name)
            print(f"\n🔍 Processing  → dataframe: {fund_name}, {fund_isin}")

            header_row_idx = next(
                (index for index, row in df.iterrows() if any("ISIN" in str(val) for val in row.dropna())),
                None
            )
            if header_row_idx is None:
                print(f"⚠️ Skipping {sheet_name} (No ISIN header found)")
                return

            df = df.dropna(how='all')
            df.reset_index(drop=True , inplace = True)
            rows = df.fillna(" ").agg(" ".join , axis = 1).apply(str.lower)
            df = df.iloc[rows[rows.apply(lambda x : "stock exchang" not in x and not ("index" in x and "stock" in x))].index.to_list()]
            df.reset_index(drop=True , inplace = True)
            grand_total_idx = rows[rows.apply(lambda x : "grand total" in x)].index.to_list()
            table_end_idx=len(df)
            if len(grand_total_idx) > 0 : 
                table_end_idx = min( table_end_idx , grand_total_idx[-1])
            
            
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

            periods = self._get_valid_periods(df , header_map)

            #process data piece by piece
            for (start_idx , end_idx) in periods:

                type_name_idx = self._get_investment_type(df , start_idx , header_map["isin"])
                if(type_name_idx == start_idx):
                    print("No valid Type Name Found. Moving on.")
                    continue
                
                type_name = df[type_name_idx:type_name_idx+1].fillna(" ").agg(" ".join , axis = 1).iloc[0]
                type_name = self.filterBullets(type_name)
                type_name = self.filterBracketContent(type_name)
                type_name = re.sub(r"[^a-zA-Z\s\&\-/\\]" , "" , type_name)
                type_name = self.filterNANIsolated(type_name)
                type_name = self.filterReccuringSpaces(type_name)

                for (index , row) in df.iloc[start_idx:min(end_idx+1,table_end_idx)].iterrows():
                    values = header_map.copy()
                    for (key , idx) in header_map.items():
                        temp = row.iloc[idx]
                        values[key] = temp
                 

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
        return  (   len(val) in range(5,20) and
                    " " not in val and 
                    (val[0].isupper() and 
                    val[1].isupper() and 
                    (val[-1].isdigit() or val[-1].upper() == "X"))
                )
                
    
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
        idx = rows[rows.apply(lambda x: "isin" in x.lower())].index.tolist()[0]
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
            if score > 0.47 :
                header_map[self.base_headers[i]] = int(idx)
        
        return header_map
    # --------OLD Functiones over--------


    def parse_all_portfolios(self):
        file_paths = self._get_file_names()
        for file_path in file_paths:
            df_raw = self._read_excel_file(file_path)
            if df_raw is None:
                continue
            for sheet_name, sheet_df in df_raw.items():
                if sheet_name not in self.sheets_to_avoid:
                    self.process_sheet(file_path, sheet_name, sheet_df)                

    def save_to_excel(self):
        error_saving = True
        while(error_saving):
            time.sleep(1)
            try:
                if not self.full_data.empty:
                    self.full_data.to_excel(self.output_file, index=False)
                    print(f"✅ Successfully saved parsed data to {self.output_file}")
                else:
                    print("⚠️ No data to save.")
                error_saving = False
            except Exception as e:
                print("error Saving [File is Open] ", e)
                continue




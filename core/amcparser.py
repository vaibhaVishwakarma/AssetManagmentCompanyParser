# core/amcparser.py
"""
AMC Portfolio Base Parser

This module defines the abstract base class `AMCPortfolioParser`, which provides
the full parsing workflow for AMC (Asset Management Company) portfolio Excel files.

Responsibilities:
- Prepare environment and clean intermediate folders.
- Load Excel files and extract fund-level information.
- Identify, map, and standardize headers using semantic similarity.
- Generate consolidated data ready for downstream processing.

All AMC-specific logic (fund name detection etc.) is meant to be implemented
in subclasses inheriting from this base parser.

Logging:
    Uses `logging` for all operational messages except for a single print
    statement (as required by design) to indicate when a sheet is processed.
"""

import re
import pandas as pd
import os
import time
from abc import ABC, abstractmethod
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from langchain_huggingface import HuggingFaceEmbeddings
import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords
import logging
import requests  # retained for compatibility with existing architecture

# -------------------------------------------------------------------
# Logger Configuration
# -------------------------------------------------------------------
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Abstract Base Class Definition
# -------------------------------------------------------------------
class AMCPortfolioParser(ABC):
    """
    Abstract base class for all AMC portfolio parsers.

    Each subclass represents a specific AMC and implements the `_get_fund_name`
    method to identify the relevant fund name in an input sheet.

    Attributes:
        amc_name (str): Name of the AMC being processed.
        data_dir (str): Directory containing source Excel files.
        output_directory (str): Destination for processed outputs.
        output_file (str): Path to the final Excel output.
        final_columns (list): List of target standardized headers.
        base_headers (list): Normalized headers used for column mapping.
        full_data (pd.DataFrame): Master dataframe containing aggregated data.
        embeddings (HuggingFaceEmbeddings): Embedding model instance.
        base_embeddings (np.ndarray): Embeddings of reference headers.
    """

    def __init__(self, amc_config, default_config, embedding_model = None):
        """
        Initialize the parser instance with configuration and embeddings setup.

        Args:
            amc_config (dict): AMC-specific configuration (data paths, exclusions).
            default_config (dict): Default global configuration values.
        """

        self.isin_pattern = r"[A-Z]{3}[A-Z0-9]{9}"



        # --------------------------------------------------------------
        # 1. Load configurations and initialize working variables
        # --------------------------------------------------------------
        self.amc_name = amc_config.get("AMCName", None)
        self.data_dir = amc_config.get("DataDirectory", f"./data/data/{self.amc_name}")
        self.output_directory = ".cleaned"
        os.makedirs(self.output_directory, exist_ok=True)
        self.sheets_to_avoid = amc_config.get("sheets_to_avoid", [])
        self.final_columns = amc_config.get("final_columns", None)

        # --------------------------------------------------------------
        # 2. Configure output and column structure
        # --------------------------------------------------------------
        self.output_file = f"{self.output_directory}/{self.amc_name}.xlsx"
        if not self.final_columns:
            self.final_columns = [
                "Name of Instrument", "ISIN", "Coupon", "Industry",
                "Quantity", "Market Value", "% to Net Assets (nav)",
                "Yield", "Yield to call (ytc)", "Yield to Maturity (ytm)"
            ]

        # normalized headers and empty master dataframe
        self.base_headers = [self._pre_process_header(h) for h in self.final_columns]
        self.full_data = pd.DataFrame(columns=self.base_headers + ["Type", "Scheme Name", "AMC"])

        # embedding setup for semantic header mapping
        self.embeddings = embedding_model if isinstance(embedding_model, HuggingFaceEmbeddings) else HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2") 
        self.base_embeddings = np.array([self._generate_embedding(h) for h in self.base_headers])

        # stopword configuration for fund name cleaning
        self.stopwords = set(stopwords.words("english"))

        # Predefined text cleaning lambdas for repeated transformations
        self.filterNonAlphaNumeric = lambda x: re.sub(r"[^a-zA-Z0-9]", "", x)
        self.filterStopWords = lambda x: " ".join(
            [w for w in str(x).lower().split() if w not in self.stopwords]
        )
        self.filterBracketContent = lambda x: re.sub(r"\([^\)]\)", "", x)
        self.filterNANIsolated = lambda x: re.sub(r"(?<!\w)(nan)+(?!\w)", "", x, flags=re.IGNORECASE)
        self.filterReccuringSpaces = lambda x: re.sub(r"\s+", " ", x)

    # -------------------------------------------------------------------
    # Utility Functions
    # -------------------------------------------------------------------
    def filterBullets(self, string):
        """Removes stray bullet-like characters or mismatched parentheses."""
        if(not isinstance(string, str)): return ""

        tmp = string + "()"
        if tmp.index("(") > tmp.index(")"):
            string = re.sub(r"[^\)]\)", "", string)
        return string

    def _create_ISIN_mapping(self, df):
        """Build dictionary mapping fund names to ISIN codes."""
        mapping = {}
        for _, row in df.iterrows():
            fn = row["Cleaned Fund Name"].lower()
            isin = row["ISIN"]
            if fn and isin and row["Growth/Regular Type"] in ["Regular", "Growth"]:
                mapping[fn] = isin
        return mapping

    def _get_file_names(self):
        """Recursively collect all valid Excel files from AMC’s data directory."""
        file_names = []
        for root, _, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith((".xlsb", ".xls", ".xlsx", ".xlsm")):
                    file_names.append(os.path.join(root, file))
        return file_names

    def _read_excel_file(self, file_path, *, sheet_name=None, header_row_idx=None):
        """Read Excel or CSV file and return all sheets as a dictionary of DataFrames."""
        try:
            ext = file_path.split(".")[-1].lower()
            if ext == "xlsb":
                return pd.read_excel(file_path, sheet_name=None, engine="pyxlsb", dtype=str)
            elif ext in ["xls", "xlsx", "xlsm"]:
                return pd.read_excel(file_path, sheet_name=None, dtype=str)
            elif ext == "csv":
                return pd.read_csv(file_path, sheet_name=sheet_name, skiprows=header_row_idx, dtype=str)
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
        return None

    # -------------------------------------------------------------------
    # Core Parsing Logic
    # -------------------------------------------------------------------

    @abstractmethod
    def _get_fund_name(df : pd.DataFrame) -> "Fund Name":
        pass

    def process_sheet(self, file_path, sheet_name, df):
        """
        Main sheet parsing logic:
        - Identify fund name and ISIN.
        - Locate and normalize header rows.
        - Extract tabular data segments between valid ISIN patterns.
        - Append processed rows to self.full_data.
        """
        logger.info(f"Processing → Sheet: {sheet_name}")
        fund_name = self._get_fund_name(df)

        if not fund_name:
            logger.warning(f"No fund name for sheet {sheet_name}")
            return

        logger.info(f"Processing DataFrame for fund: {fund_name}")

        # locate header row index (first row containing "ISIN")
        header_row_idx = next(
            (idx for idx, row in df.iterrows() if any("ISIN" in str(v) for v in row.dropna())),
            None
        )
        if header_row_idx is None:
            logger.warning(f"Skipping {sheet_name} (no ISIN header found)")
            return

        # basic cleaning: drop empty rows and irrelevant sections
        df = df.dropna(how="all").reset_index(drop=True)
        rows = df.fillna(" ").agg(" ".join, axis=1).apply(str.lower)
        df = df.iloc[
            rows[rows.apply(lambda x: "stock exchang" not in x and not ("index" in x and "stock" in x))].index
        ].reset_index(drop=True)

        # locate table end boundary
        grand_total_idx = rows[rows.apply(lambda x: "grand total" in x)].index.to_list()
        table_end_idx = min(grand_total_idx[-1], len(df)) if grand_total_idx else len(df)

        # header normalization
        header_row = self._fetch_header_row(df)

        # merge and clean NULL header segments
        n_iter = 0
        while "NULL" in header_row and n_iter < 5:
            start = None
            end = len(header_row)
            for i in range(len(header_row)):
                if start == None and header_row[i] != "NULL":
                    start = i
                    break
            for i in range(start + 1 , len(header_row)):
                if header_row[i] != "NULL":
                    end = i
                    break
            alter1 = df.iloc[:,start:end].fillna("").agg(" ".join,axis = 1)
            alter2 = df.drop(df.columns[start:end],axis = 1)
            df = pd.concat([alter1 , alter2] , axis = 1)

            header_row = self._fetch_header_row(df)
            n_iter+=1

        header_map = self._header_mapper(header_row)
        periods = self._get_valid_periods(df, header_map)
        # iterate through all valid ISIN-segmented regions
        for start_idx, end_idx in periods:
            type_name_idx = self._get_investment_type(df, start_idx, header_map["isin"])
            if type_name_idx == start_idx:
                logger.info("No valid Type Name found. Skipping section.")
                continue

            type_name = df.iloc[type_name_idx, :].fillna(" ").agg(" ".join, axis=0)
            # type_name = df.iloc[type_name_idx, :].fillna(" ").apply(lambda x: " ".join(x), axis=1)

            type_name = df[type_name_idx:type_name_idx+1].fillna(" ").agg(" ".join , axis = 1).iloc[0]
            type_name = self.filterBullets(type_name)
            type_name = self.filterBracketContent(type_name)
            type_name = re.sub(r"[^a-zA-Z\s\&\-/\\]" , "" , type_name)
            type_name = self.filterNANIsolated(type_name)
            type_name = self.filterReccuringSpaces(type_name)
            type_name = type_name if "total" not in type_name.lower() else "uncategorised"


            for _, row in df.iloc[start_idx:min(end_idx + 1, table_end_idx)].iterrows():
                values = header_map.copy()
                for key, idx in header_map.items():
                    values[key] = row.iloc[idx]
                values["Type"] = type_name
                values["Scheme Name"] = fund_name
                values["AMC"] = self.amc_name
                self.full_data = pd.concat([self.full_data, pd.DataFrame([values])], ignore_index=True).drop_duplicates()

        # required print output
        print("Processed this Excel")
        logger.info("Completed sheet parsing.")

    # -------------------------------------------------------------------
    # Helper Methods for Internal Use
    # -------------------------------------------------------------------

    def _get_investment_type(self, df , start_index, isin_col_num):
        n_iter = 1 # to avoid endless loop
        while(n_iter<=10):
            candidate_isin = df.iloc[start_index-n_iter , isin_col_num]
            print(candidate_isin)
            if not self._check_isin(str(candidate_isin)):
                return start_index-n_iter
            n_iter+=1
        return start_index
    
    def _filter_isin(self, string):
        if(not isinstance(string, str)):
            return ""
        string = string.strip().upper()
        return re.sub(r"[^A-Z0-9]","",string)
        
    def _check_isin(self, val):
        val = self._filter_isin(val)
        return bool(re.search(self.isin_pattern, val))
    
    def _clean_fund_name(self,name):
        return re.sub(r"[^a-zA-z0.9\+\-\\\(\)\s/]","",name)
    
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

    def _generate_embedding(self, text: str) -> list[float]:
        """Generate vector embedding for a given text string."""
        return self.embeddings.embed_query(text)

    def _fetch_header_row(self, df: pd.DataFrame) -> list[str]:
        """Identify the row containing 'ISIN' and return its cell values."""
        rows = df.astype(str).agg(" ".join, axis=1)
        idx = rows[rows.apply(lambda x: "isin " in x.lower())].index.tolist()[0]
        header_row = df.iloc[idx, :].fillna("NULL")
        return list(header_row)

    def _header_mapper(self, header_row) -> dict:
        """Map target headers to actual sheet columns using cosine similarity."""
        header_map = {}
        self.base_headers = [self._pre_process_header(h) for h in self.base_headers]
        header_row = [self._pre_process_header(h) for h in header_row]
        header_row_embeddings = np.array([self._generate_embedding(h) for h in header_row])
        similarity_matrix = cosine_similarity(self.base_embeddings, header_row_embeddings)
        most_similar_indices = np.argmax(similarity_matrix, axis=1)
        most_similar_scores = np.max(similarity_matrix, axis=1)

        for i, (idx, score) in enumerate(zip(most_similar_indices, most_similar_scores)):
            bh, hr = self.base_headers[i], header_row[idx]
            # heuristic adjustments for specific header types
            if "yield" in bh and "yield" not in hr:
                score = 0
            if bh == "yield" and any(k in hr for k in ["ytc", "call", "ytm"]):
                score = 0
            if ("ytc" in bh or "call" in bh):
                score = 0
            if ("ytc" in bh or "call" in bh) and ("ytc" in hr or "call" in hr) and "put" not in hr:
                score = 1
            if ("ytm" in bh or "maturity" in bh) :
                score = 0
            if ("ytm" in bh or "maturity" in bh) and ("ytm" in hr or "maturity" in hr):
                score = 1
            if "coupon" in bh and "coupon" not in hr:
                score = 0

            if score > 0.47:
                header_map[bh] = int(idx)
            logger.info(f"Mapped base '{bh}' → header '{hr}' (score={score:.4f})")

        return header_map

    def _pre_process_header (self, x) :
        return re.sub(r"[^a-z\s%\(\)\\/]","",x.lower())

    # -------------------------------------------------------------------
    # Orchestration Methods
    # -------------------------------------------------------------------
    def parse_all_portfolios(self):
        """Iterate through all AMC Excel files and parse each sheet."""
        for file_path in self._get_file_names():
            df_raw = self._read_excel_file(file_path)
            if not df_raw:
                continue
            for sheet_name, sheet_df in df_raw.items():
                if sheet_name not in self.sheets_to_avoid:
                    self.process_sheet(file_path, sheet_name, sheet_df)

    def save_to_excel(self):
        """Persist final combined dataframe to an Excel file."""
        while True:
            try:
                if not self.full_data.empty:
                    self.full_data.to_excel(self.output_file, index=False)
                    logger.info(f"Saved parsed data → {self.output_file}")
                else:
                    logger.warning("No data available to save.")
                break
            except Exception as e:
                logger.error(f"Save failed (file open?): {e}")
                time.sleep(1)

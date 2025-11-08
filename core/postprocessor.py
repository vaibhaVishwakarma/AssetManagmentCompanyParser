# core/postprocessor.py
import os
import re
import yaml
import logging
import pandas as pd
import numpy as np
from fuzzywuzzy import process

logger = logging.getLogger(__name__)

class PortfolioPostProcessor:
    """Unified post-processor combining data cleaning and final compilation."""

    def __init__(self, config_path="./config/amc_configs.yaml"):
        with open(config_path, "r") as f:
            self.configs = yaml.safe_load(f)
        self.output_folder = ".cleaned"
        self.final_folder = ".final_cleaned"
        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs(self.final_folder, exist_ok=True)
        logger.info("Initialized PortfolioPostProcessor")

    # ---------- Data Cleaning ----------
    def clean_data(self):
        floatFilter = lambda x: re.findall(r"-?\d+\.\d+", str(x))
        integerFilter = lambda x: re.findall(r"-?\d+", str(x))

        def cleanString(s):
            s = re.sub("%", "", str(s))
            try:
                return float(s)
            except Exception:
                pass
            s1 = floatFilter(s)
            if s1:
                return float(s1[0])
            s2 = integerFilter(s)
            if s2:
                return float(s2[0])
            return 0.0

        def dropCriteria(row):
            quantity, mkt, nav = row.iloc[4:7]
            to_keep = not ((nav > 100.0) or (quantity == 0 and mkt == 0))
            return to_keep

        files = [f for f in os.listdir(self.output_folder) if "~" not in f]
        logger.info(f"Cleaning {len(files)} intermediate Excel files")

        for filename in files:
            file_path = os.path.join(self.output_folder, filename)
            df = pd.read_excel(file_path)
            amc_name = df["AMC"].unique()[0]
            config = self.configs.get(amc_name, {})
            logger.info(f"Cleaning data for AMC: {amc_name}")

            scale_by_hundred = [s.lower() for s in config.get("Scale100", [])]
            factor_by_hundred = [s.lower() for s in config.get("Scale100th", [])]

            numeric_columns = [
                "coupon", "quantity", "market value (mkt) ( rs lakh )",
                "% to net assets (nav)", "yield", "yield to call (ytc)"
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].map(cleanString).astype(np.float64)
                    if col in factor_by_hundred:
                        df[col] = df[col] / 100
                    if col in scale_by_hundred:
                        df[col] = df[col] * 100
                    df[col] = df[col].fillna(0)

            df["isin"] = df["isin"].apply(lambda x: str(x)[:12])
            if "% to net assets (nav)" in df.columns:
                df["% to net assets (nav)"] = 100 * df["% to net assets (nav)"]
            if "yield" in df.columns:
                df["yield"] = 100 * df["yield"]
            if "yield to call (ytc)" in df.columns:
                df["yield to call (ytc)"] = 100 * df["yield to call (ytc)"]

            df = df[df.apply(dropCriteria, axis=1)]
            df.to_excel(os.path.join(self.final_folder, filename), index=False)
            logger.info(f"Cleaned and saved: {filename}")

    # ---------- Final Compilation ----------
    def compile_final_output(self):
        files = [f for f in os.listdir(self.final_folder) if "~" not in f]
        if not files:
            logger.warning("No cleaned files found to compile.")
            return

        df = pd.concat(
            [pd.read_excel(os.path.join(self.final_folder, f)) for f in files],
            ignore_index=True
        )

        logger.info(f"Compiling {len(files)} cleaned files into unified dataset")

                
        category_mapping = {
            # Equity & Equity Related
            'EQUITY & EQUITY RELATED': 'Equity & Equity Related',
            'Equity & Equity Related': 'Equity & Equity Related',
            'Equity & Equity related': 'Equity & Equity Related',
            'Equity & Equity Related Instruments': 'Equity & Equity Related',
            'Equity & Equity related Aerospace & Defense': 'Equity & Equity Related',
            'Equity & Equity related Automobiles': 'Equity & Equity Related',
            'Equity & Equity related Banks': 'Equity & Equity Related',
            'Equity & Equity related Biotechnology': 'Equity & Equity Related',
            'Equity & Equity related Capital Markets': 'Equity & Equity Related',
            'Equity & Equity related Construction': 'Equity & Equity Related',
            'Equity & Equity related Consumer Durables': 'Equity & Equity Related',
            'Equity & Equity related Electrical Equipment': 'Equity & Equity Related',
            'Equity & Equity related Exchange Traded Funds': 'Equity & Equity Related',
            'Equity & Equity related Finance': 'Equity & Equity Related',
            'Equity & Equity related IT - Software': 'Equity & Equity Related',
            'Equity & Equity related Industrial Products': 'Equity & Equity Related',
            'Equity & Equity related Pharmaceuticals & Biotechnology': 'Equity & Equity Related',
            'Equity & Equity related Realty': 'Equity & Equity Related',
            'Equity & Equity related Semiconductors': 'Equity & Equity Related',
            'Equity Shares': 'Equity & Equity Related',
            'Equity shares': 'Equity & Equity Related',
            'FOREIGNEQUITYSECURITIES': 'Equity & Equity Related',
            'Foreign Securities - Equity': 'Equity & Equity Related',
            'EQUITYEQUITYRELATED': 'Equity & Equity Related',

            # Exchange Traded Funds (ETFs)
            'Exchange Traded Fund': 'Exchange Traded Funds (ETFs)',
            'Exchange Traded Funds': 'Exchange Traded Funds (ETFs)',
            'EXCHANGE TRADED FUND UNITS': 'Exchange Traded Funds (ETFs)',
            'ETF': 'Exchange Traded Funds (ETFs)',
            'International Exchange Traded Funds': 'Exchange Traded Funds (ETFs)',
            'International Exchange Traded Funds Exchange Traded Funds': 'Exchange Traded Funds (ETFs)',
            'Foreign Securities and/or overseas ETF': 'Exchange Traded Funds (ETFs)',
            'Foreign Securities/Overseas ETFs': 'Exchange Traded Funds (ETFs)',
            'FOREIGNETF': 'Exchange Traded Funds (ETFs)',

            # Bonds & Debentures
            'BOND & NCDs': 'Bonds & Debentures',
            'Bonds': 'Bonds & Debentures',
            'Debentures and Bonds': 'Bonds & Debentures',
            'Zero Coupon Bond': 'Bonds & Debentures',
            'Zero Coupon Bonds': 'Bonds & Debentures',
            'Zero Coupon Bonds / Deep Discount Bonds': 'Bonds & Debentures',
            'Non Convertible Debentures': 'Bonds & Debentures',
            'Non Convertible Debentures / Bonds': 'Bonds & Debentures',
            'NON-CONVERTIBLE DEBENTURES/BONDS/ZCB': 'Bonds & Debentures',
            'II NON-CONVERTIBLE DEBENTURES/BONDS': 'Bonds & Debentures',
            'II NON-CONVERTIBLE DEBENTURES/BONDS/ZCB': 'Bonds & Debentures',
            'Convertible Debenture': 'Bonds & Debentures',
            'Compulsory Convertible Debenture': 'Bonds & Debentures',
            'COMPULSORILYCONVERTIBLEDEBENTURE': 'Bonds & Debentures',
            'Fixed rates bonds - Corporate': 'Bonds & Debentures',
            'Debt Instruments': 'Bonds & Debentures',
            'Debt Instruments SOVEREIGN': 'Bonds & Debentures',
            'DEBT INSTRUMENTS': 'Bonds & Debentures',
            'DEBTINSTRUMENTS': 'Bonds & Debentures',
            'Government Bonds': 'Bonds & Debentures',
            'Government Securities': 'Bonds & Debentures',
            'Government Securities / SDL': 'Bonds & Debentures',
            'Government Securities Central/State': 'Bonds & Debentures',
            'Government Securities Central/State Cash & Equivalent': 'Bonds & Debentures',
            'State Government Securities': 'Bonds & Debentures',
            'Govt Security': 'Bonds & Debentures',
            'Govt Securities / SDL': 'Bonds & Debentures',
            'Central Government Securities': 'Bonds & Debentures',
            'GOVERNMENT SECURITIES': 'Bonds & Debentures',
            'GOVERNMENTSECURITIES': 'Bonds & Debentures',
            'GOVERNMENTSECURITIESCENTRALSTATE': 'Bonds & Debentures',
            'i Government Securities': 'Bonds & Debentures',
            'ii State Government Securities': 'Bonds & Debentures',

            # Money Market Instruments
            'Certificate of Deposit': 'Money Market Instruments',
            'Certificate of Deposits': 'Money Market Instruments',
            'Certificate of Deposit IND A': 'Money Market Instruments',
            'Certificate of Deposits C': 'Money Market Instruments',
            'CERTIFICATEOFDEPOSIT': 'Money Market Instruments',
            'CERTIFICATEOFDEPOSITCD': 'Money Market Instruments',
            'CD-Certificate of Deposits': 'Money Market Instruments',
            'Commercial Paper': 'Money Market Instruments',
            'Commercial Papers': 'Money Market Instruments',
            'Commercial Papers C': 'Money Market Instruments',
            'COMMERCIALPAPER': 'Money Market Instruments',
            'COMMERCIALPAPERSCP': 'Money Market Instruments',
            'Treasury Bill': 'Money Market Instruments',
            'Treasury Bills': 'Money Market Instruments',
            'Treasury Bill/Cash Management Bill': 'Money Market Instruments',
            'Treasury Bill/Cash Management Bill CRISIL A': 'Money Market Instruments',
            'Treasury Bill/Cash Management Bill SOVEREIGN': 'Money Market Instruments',
            'TREASURYBILL': 'Money Market Instruments',
            'TREASURYBILLS': 'Money Market Instruments',
            'T-Bil': 'Money Market Instruments',
            'Tri Party Repo TREPs': 'Money Market Instruments',
            'TREPS / Reverse Repo': 'Money Market Instruments',
            'Reverse Repo / TREPS': 'Money Market Instruments',
            'Reverse Repo --': 'Money Market Instruments',

            # Alternative Investments
            'Alternative Investment Fund': 'Alternative Investments',
            'Alternative Investment Fund Units': 'Alternative Investments',
            'Alternative Investment Funds': 'Alternative Investments',
            'Alternative Investment Funds AIF': 'Alternative Investments',
            'Units of an Alternative Investment Fund AIF': 'Alternative Investments',
            'Investment in AIF': 'Alternative Investments',
            'AIF CAT': 'Alternative Investments',
            'ALTERNATIVEINVESTMENTFUND': 'Alternative Investments',
            'ALTERNATIVEINVESTMENTFUNDUNITS': 'Alternative Investments',
            'CDMDFAIF': 'Alternative Investments',
            'Infrastructure Investment Trusts': 'Alternative Investments',
            'Units of Infrastructure Investment Trusts InvITs': 'Alternative Investments',
            'InvIT': 'Alternative Investments',
            'Invits': 'Alternative Investments',
            'Units issued by REITs & InvITs': 'Alternative Investments',
            'REIT': 'Alternative Investments',
            'Reits': 'Alternative Investments',
            'ReIT': 'Alternative Investments',
            'Units of Real Estate Investment Trust REITs': 'Alternative Investments',
            'Real Estate Investment Trusts': 'Alternative Investments',
            'Real Estate Investment Trust': 'Alternative Investments',
            'UNITS OF INVIT': 'Alternative Investments',
            'UNITS OF REIT': 'Alternative Investments',
            'UNITSISSUEDBYINVIT': 'Alternative Investments',
            'UNITSISSUEDBYREIT': 'Alternative Investments',
            'BUNITSOFREALESTATEINVESTMENTTRUSTSREITS': 'Alternative Investments',

            # Other/Uncategorized
            'Others': 'Other/Uncategorized',
            'OTHERS': 'Other/Uncategorized',
            'UNLISTED': 'Other/Uncategorized',
            'Unlisted': 'Other/Uncategorized',
            'Privately Placed / Unlisted': 'Other/Uncategorized',
            'Privately Placed/Unlisted': 'Other/Uncategorized',
            'Privately placed / Unlisted': 'Other/Uncategorized',
            'Option wise per unit Net Asset Values are as follows': 'Other/Uncategorized',
            'ISINCODE securityname Yield to Call': 'Other/Uncategorized',
            'Arbitrage': 'Other/Uncategorized',
            'Numero Uno International Ltd Finance e-': 'Other/Uncategorized',
            'Margin Mutual Fund Units': 'Other/Uncategorized',
            'RISKLEVELBASEDONPORTFOLIOASONMAY': 'Other/Uncategorized',
            'RISKLEVELOFTIERBENCHMARKASONMAY': 'Other/Uncategorized',
            'CMSI': 'Other/Uncategorized',
            'CNX NIFTY-JUN - - -': 'Other/Uncategorized',
            'Amount Rs in Lakhs to NAV': 'Other/Uncategorized',
            'uncategorised': 'Other/Uncategorized'
        }

        keys = list(category_mapping.keys())

        def fuzzy_map_entries_to_category(entries):
            results = {}
            for entry in entries:
                if not keys:
                    results[entry] = "others"
                else:
                    match, score = process.extractOne(entry, keys)
                    results[entry] = category_mapping.get(match, "others")
            return results

        if "Type" in df.columns:
            df["Type"] = df["Type"].map(
                fuzzy_map_entries_to_category(np.unique(df["Type"]))
            )

        if "yield to call (ytc)" in df.columns:
            df = df.drop("yield to call (ytc)", axis=1)

        new_cols = [
            "Name of Instrument", "ISIN", "Coupon", "Industry", "Quantity",
            "Market Value", "% to Net Assets", "Yield", "Type",
            "Scheme Name", "AMC", "Scheme ISIN"
        ]

        if len(df.columns) == len(new_cols):
            df.columns = new_cols

        df.to_csv("Regular_combined_sheet_reduced_types.csv", index=False)
        logger.info("Final combined CSV generated successfully.")

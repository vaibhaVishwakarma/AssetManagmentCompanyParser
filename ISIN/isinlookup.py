
import pandas as pd
import re

def create_ISIN_mapping(df):
        
        """Create a mapping of fund names to ISINs."""
        isin_mapping = {}
        for index, row in df.iterrows():
            fund_name = row['Cleaned Fund Name'].lower()
            isin = row['ISIN']
            if fund_name and isin and row['Growth/Regular Type'] in ["Growth", "Regular"]:
                isin_mapping[fund_name] = isin
        return isin_mapping

df =pd.read_excel("ISIN/fund_isin.xlsx")
lookup= create_ISIN_mapping(df)
fund_name="hdfc balanced advantage fund"
print(fund_name.lower())

if fund_name.lower() in lookup:
    print(f"ISIN for {fund_name.lower()}: {lookup[fund_name.lower()]}")
else:
    print(f"No ISIN found for {fund_name}")
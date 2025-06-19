from eparse.core import get_df_from_file, df_serialize_table
import re
import pandas as pd
import os

datadir='data/data/Kotak Mutual Fund/'
filename='ConsolidatedSebiPortfolioJanuary2025.xls'
output_file = 'kotak_mutual_fund_porfolio.xlsx'
AMC_NAME="Kotak Mutual Fund"
datafile=os.path.join(datadir, filename)


def read_excel_file(file_path):
        """Handles reading `.xlsb`, `.xls`, and `.xlsx` files dynamically."""
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
        


#get the fund names 
fund_names=pd.read_excel(datafile, sheet_name="Scheme", dtype=str)
fund_names.columns = fund_names.iloc[0]
fund_names = fund_names[1:].reset_index(drop=True)
fund_names=dict(zip(fund_names['Abbreviations'], fund_names['Scheme Name']))

df_raw=read_excel_file(datafile)
sheets_to_avoid=["ESG", "Scheme", "Common Notes"] #avoid ESG fund due to different format


full_data=pd.DataFrame()

for sheet_name, sheet_df in df_raw.items():
                
        if sheet_name not in sheets_to_avoid:
                print(f"\n🔍 Processing  → Sheet: {sheet_name}")

                fund = fund_names.get(sheet_name, None)

                if fund is not None and sheet_name:
                    print(f"\n🔍 Processing  → Sheet: {fund}")


                    header_row_idx = next(
                        (index for index, row in sheet_df.iterrows() if any("ISIN" in str(val) for val in row.dropna())),
                        None
                    )
                    if header_row_idx is None:
                        print(f"⚠️ Skipping {sheet_name} (No ISIN header found)")
                        continue

                    df_clean = pd.read_excel(datafile, sheet_name=sheet_name, skiprows=header_row_idx, dtype=str)
                    df_clean.columns = df_clean.iloc[0]
                    df_clean = df_clean[1:].reset_index(drop=True)

                    df_clean.columns = ["Type", "Coupon", "Name of Instrument", "ISIN", "Industry", "Yield", "Quantity", "Market Value (Rs.in Lacs)", "% to Net Assets"]
                    df_clean = df_clean.rename(columns={"Market Value (Rs.in Lacs)": "Market Value"})
                    #df_clean = df_clean.dropna(subset=["ISIN", "Name of Instrument"])
                    a=df_clean.index[df_clean['Coupon'] == "Listed/Awaiting listing on Stock Exchange"].tolist()
                    for i in a:
                        print(i)
                        type=df_clean.at[i-1, "Coupon"] 
                        df_clean.at[i, "Type"] = type
                    df_clean[['Type']] = df_clean[['Type']].fillna(method='ffill')  #set the type for the listed/awaiting listing on stock exchange
                    df_clean[['Yield']] = df_clean[['Yield']].fillna(value=0)
                    df_clean[['Coupon']] = df_clean[['Coupon']].fillna(value=0)


                    df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                    df_clean = df_clean.round(2)
                    df_clean["Scheme Name"] = fund
                    df_clean["AMC"] = AMC_NAME
                    full_data=pd.concat([full_data,df_clean],ignore_index=True) if not full_data.empty else df_clean



full_data.to_excel(output_file, index=False)
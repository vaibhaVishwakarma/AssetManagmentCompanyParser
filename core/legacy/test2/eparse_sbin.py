from eparse.core import get_df_from_file, df_serialize_table
import re
import pandas as pd
import os

#Note 
# Long short is by type see
# default logic by fund type 


datadir='/Users/njp60/Documents/code/mutualfundbackend/funddata/data/SBI Mutual Fund/'
filename='All-Schemes-Monthly-Portfolio---as-on-31st-January-2025.xlsx'
output_file = 'sbi_mutual_fund_porfolio.xlsx'
AMC_NAME="SBI Mutual Fund"
datafile=os.path.join(datadir, filename)


def clean_fund_name(fund_name):
    # Use regex to keep everything before 'fund' (including 'fund') and remove everything after it
    cleaned_name = re.sub(r'\s+fund.*', ' fund', fund_name, flags=re.IGNORECASE)
    return cleaned_name.strip()


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
fund_names=pd.read_excel(datafile, sheet_name="Index", dtype=str)
fund_names.columns = fund_names.iloc[1]
fund_names = fund_names[2:].reset_index(drop=True)
print(fund_names.head(10))
fund_names=dict(zip(fund_names['Scheme Short code'], fund_names['Scheme Name']))


df_raw=read_excel_file(datafile)
sheets_to_avoid=[] #avoid ESG fund due to different format


full_data=pd.DataFrame()

for sheet_name, sheet_df in df_raw.items():
                
        if sheet_name is not None and sheet_name not in sheets_to_avoid:
                print(f"\n🔍 Processing  → Sheet: {sheet_name}")

                fund = fund_names.get(sheet_name, None)
                fund=clean_fund_name(fund) if fund else None

                if fund is not None and sheet_name:
                    print(f"\n🔍 Processing  → Fund: {fund}")


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

                    df_clean = df_clean.loc[:, df_clean.columns.notna()]

                    # Remove columns with 
                    columns_to_remove = ['YTC % ##', 'Notes & Symbols', 'BRSR Scores \n(also called as ESG Scores) ',
       'BRSR Core Scores(also called as ESG Core Scores) ',
       'Link to BRSR disclosures', 'Link to BRSR Core Assured disclosures', 'BRSR Core Scores\n(also called as ESG Core Scores) ']
                    
                    
                    for col in df_clean.columns:
                       if col in columns_to_remove:
                            df_clean = df_clean.drop(columns=col)

                    print(df_clean.columns)

                    col_names=[ "Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets", "Yield"]
                    df_clean.columns =col_names
                    df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)


                    #Just a simple logic to determine the type of instrument need to update later TODO

                    df_clean[['Yield']] = df_clean[['Yield']].fillna(value=0)
                    df_clean['Type'] = df_clean['Yield'].apply(lambda x: 'Debt' if x != 0 else 'Equity or Equity related')
                    
                    df_clean = df_clean.round(2)
                    df_clean["Scheme Name"] = fund
                    df_clean["AMC"] = AMC_NAME

                    print(df_clean.head(200))
                    full_data=pd.concat([full_data,df_clean],ignore_index=True) if not full_data.empty else df_clean


full_data.to_excel(output_file, index=False)



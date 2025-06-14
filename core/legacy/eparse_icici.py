from eparse.core import get_df_from_file, df_serialize_table
import re
import pandas as pd
import os

#Note 
# edge case is detail of long and short information e.g nippon india balance fund & see example output on growww
#Fund: Nippon India Overnight Fund
#Type: adjustments 
#other notes

datadir='/Users/njp60/Documents/code/mutualfundbackend/funddata/data/ICICI Prudential Mutual Fund/'
filename='NIMF_MONTHLY_PORTFOLIO_31-Jan-25.xls'
output_file = 'icici_mutual_fund_porfolio.xlsx'
AMC_NAME="ICICI Prudential Mutual Fund"
datafile=os.path.join(datadir, filename)


#Notes todo column name header for coupoons is not working 

def get_file_names(file_path):
    """ Extract excel file names from the given path. """
    file_names = []
    for root, dirs, files in os.walk(file_path):
        for file in files:
            if file.endswith(('.xlsb', '.xls', '.xlsx')):
                file_names.append(os.path.join(root, file))

    return file_names
    



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
        

def get_fund_name(sheet_df: pd.DataFrame, amc_name: str):
    """
    Extract the scheme name from the first 6 rows of sheet_df by:
      1) Finding any cell containing "fund" (case‑insensitive).
      2) Excluding any cell exactly equal to the given amc_name.
      3) Returning the longest remaining string, trimmed of extra whitespace/punctuation.
    """
    # normalize AMC name for comparison
    amc_norm = amc_name.strip().lower()

    # look only at first 6 rows, convert all to strings
    top6 = sheet_df.head(6).astype(str)

    # collect any cell containing "fund" but not equal to AMC name
    candidates = []
    for cell in top6.values.flatten():
        txt = cell.strip()
        low = txt.lower()
        if "fund" in low and low != amc_norm:
            candidates.append(txt)

    if not candidates:
        return None

    # pick the longest candidate
    raw = max(candidates, key=len)

    # clean up whitespace & trailing punctuation
    name = re.sub(r'\s+', ' ', raw).strip()
    name = re.sub(r'[—–\-\:\;,\s]+$', '', name)
    return name



filenames=get_file_names(datadir)


#get the fund names 
#fund_names=pd.read_excel(datafile, sheet_name="INDEX", dtype=str)
#fund_names.columns = ["Short Name", "Scheme Name"]
#fund_names.columns = fund_names.iloc[0]
#fund_names = fund_names[1:].reset_index(drop=True)
#fund_names=dict(zip(fund_names['Short Name'], fund_names['Scheme Name']))

full_data=pd.DataFrame()


for datafile in filenames:

    df_raw=read_excel_file(datafile)
    sheets_to_avoid=[] #avoid ESG fund due to different format
    for sheet_name, sheet_df in df_raw.items():
                
        if sheet_name not in sheets_to_avoid:
                print(f"\n🔍 Processing  → Sheet: {sheet_name}")

                #fund = fund_names.get(sheet_name, None)
                fund = get_fund_name(sheet_df, AMC_NAME)

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

                    print(df_clean.columns)

                    if "Coupon" not in df_clean.columns:
                        df_clean.insert(3, 'Coupon','0')
                        df_clean['Coupon'] = 0

                    
                    col_names=["Name of Instrument","ISIN", "Coupon" ,"Industry", "Quantity", "Market Value", "% to Net Assets", "Yield", "Yield to call"]
                    
                    if len(df_clean.columns) >10:
                        print("⚠️ Skipping {sheet_name} (Too many columns) probably EGS fund)")
                        continue

                    df_clean.columns =col_names
                    df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)


                    #Just a simple logic to determine the type of instrument need to update later TODO

                    df_clean[['Yield']] = df_clean[['Yield']].fillna(value=0)
                    df_clean['Type'] = df_clean['Yield'].apply(lambda x: 'Debt or related' if x != 0 else 'Equity or Equity related')
                    
                    df_clean = df_clean.round(2)
                    df_clean["Scheme Name"] = fund
                    df_clean["AMC"] = AMC_NAME

                    print(df_clean.head(200))
                    full_data=pd.concat([full_data,df_clean],ignore_index=True) if not full_data.empty else df_clean


                    
                    
                    




full_data.head(500)

full_data.to_excel(output_file, index=False)



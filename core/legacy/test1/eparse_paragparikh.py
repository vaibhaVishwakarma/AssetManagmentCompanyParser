from eparse.core import get_df_from_file, df_serialize_table
import re
import pandas as pd
import os




def get_file_names(file_path):
    """ Extract excel file names from the given path. """
    file_names = []
    for root, dirs, files in os.walk(file_path):
        for file in files:
            if file.endswith(('.xlsb', '.xls', '.xlsx')):
                file_names.append(os.path.join(root, file))

    return file_names



datadir='/Users/njp60/Documents/code/mutualfundbackend/funddata/data/PPFAS Mutual Fund/'
filename=get_file_names(datadir)[0]
output_file = 'paragparikh_fund_porfolio.xlsx'
AMC_NAME="Parag Parikh FAS Mutual Fund"
datafile=os.path.join(datadir, filename)
print(datafile)



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
fund_names=dict(zip(fund_names['Short Name'], fund_names['Scheme Name']))
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

                    df_clean= df_clean.loc[:, df_clean.columns.notna()]

                    print(df_clean.head(10))

                    df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value (Rs.in Lacs)", "% to Net Assets", "Yield", "Yield 2"]
                    df_clean = df_clean.rename(columns={"Market Value (Rs.in Lacs)": "Market Value"})
                    df_clean = df_clean.drop(columns=["Yield 2"])
                   

                    df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)

                                #Just a simple logic to determine the type of instrument need to update later TODO

                    df_clean[['Yield']] = df_clean[['Yield']].fillna(value=0)
                    df_clean['Type'] = df_clean['Yield'].apply(lambda x: 'Debt or related' if x != 0 else 'Equity or Equity related')


                    df_clean = df_clean.round(2)
                    df_clean["Scheme Name"] = fund
                    df_clean["AMC"] = AMC_NAME
                    full_data=pd.concat([full_data,df_clean],ignore_index=True) if not full_data.empty else df_clean



full_data.to_excel(output_file, index=False)
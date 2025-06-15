import pandas as pd
import os


# This function is designed to clean and process the data from Parag Parikh Mutual Fund's portfolio sheets adapated from eparse_parag_parikh.py in legacy folder 
# Create similar functions for other AMCs 
# Some of the AMC we have individual file avaliable as well

def clean_parag_parikh(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
 

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

def clean_icici(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = df_clean['Industry'].apply(lambda x: 'Debt or related' if 'DEBT' in str(x).upper() else 'Equity or Equity related')
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_mirae(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = 'Equity or Equity related'  # Mirae is primarily equity focused
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_quant(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = df_clean['Industry'].apply(lambda x: 'Debt or related' if 'DEBT' in str(x).upper() else 'Equity or Equity related')
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_sbin(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = df_clean['Industry'].apply(lambda x: 'Debt or related' if 'DEBT' in str(x).upper() else 'Equity or Equity related')
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_nippon(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = df_clean['Industry'].apply(lambda x: 'Debt or related' if 'DEBT' in str(x).upper() else 'Equity or Equity related')
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_axis(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = df_clean['Industry'].apply(lambda x: 'Debt or related' if 'DEBT' in str(x).upper() else 'Equity or Equity related')
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_kotak(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = df_clean['Industry'].apply(lambda x: 'Debt or related' if 'DEBT' in str(x).upper() else 'Equity or Equity related')
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_hdfc(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                df_clean.columns = ["Name of Instrument", "ISIN", "Industry", "Quantity", "Market Value", "% to Net Assets"]
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                df_clean['Type'] = df_clean['Industry'].apply(lambda x: 'Debt or related' if 'DEBT' in str(x).upper() else 'Equity or Equity related')
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_bankofindia(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Name of Security': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (In Lakhs)': 'Market Value',
                    'Market Value (Rs. Lakhs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of Net Assets': '% to Net Assets',
                    'Percentage to NAV': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Asset Class' in df_clean.columns:
                    df_clean['Type'] = df_clean['Asset Class'].apply(
                        lambda x: 'Debt or related' if 'debt' in str(x).lower() else 'Equity or Equity related'
                    )
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_canerarobeco(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Name of Security': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (In Lakhs)': 'Market Value',
                    'Market Value (Rs. Lakhs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of Net Assets': '% to Net Assets',
                    'Percentage to NAV': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Instrument Type' in df_clean.columns:
                    df_clean['Type'] = df_clean['Instrument Type'].apply(
                        lambda x: 'Debt or related' if 'debt' in str(x).lower() else 'Equity or Equity related'
                    )
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_dsp(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Name of Security': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (In Lakhs)': 'Market Value',
                    'Market Value (Rs. Lakhs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of Net Assets': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Asset Type' in df_clean.columns:
                    df_clean['Type'] = df_clean['Asset Type'].apply(
                        lambda x: 'Debt or related' if 'debt' in str(x).lower() else 'Equity or Equity related'
                    )
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_invesco(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Name of Instrument/Issue': 'Name of Instrument',
                    'Security Name': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (in Lakhs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of Net Assets': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Instrument Type' in df_clean.columns:
                    df_clean['Type'] = df_clean['Instrument Type'].apply(
                        lambda x: 'Debt or related' if 'debt' in str(x).lower() else 'Equity or Equity related'
                    )
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_jmfinancial(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Name of Security': 'Name of Instrument',
                    'Industry Classification': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (Rs. Lakhs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of NAV': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Coupon Rate' in df_clean.columns:
                    df_clean['Yield'] = pd.to_numeric(df_clean['Coupon Rate'].str.replace('%', ''), errors='coerce').fillna(0)
                    df_clean['Type'] = df_clean['Yield'].apply(lambda x: 'Debt or related' if x != 0 else 'Equity or Equity related')
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_lic(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Name of Instrument/Issue': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs.)': 'Market Value',
                    'Market Value (in Rs.)': 'Market Value',
                    '% to NAV': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Coupon' in df_clean.columns or 'Rate (%)' in df_clean.columns:
                    coupon_col = 'Coupon' if 'Coupon' in df_clean.columns else 'Rate (%)'
                    df_clean['Yield'] = pd.to_numeric(df_clean[coupon_col], errors='coerce').fillna(0)
                    df_clean['Type'] = df_clean['Yield'].apply(lambda x: 'Debt or related' if x != 0 else 'Equity or Equity related')
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_motilaloswal(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (Rs.in Lacs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of Net Assets': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Coupon Rate (%)' in df_clean.columns:
                    df_clean['Yield'] = pd.to_numeric(df_clean['Coupon Rate (%)'], errors='coerce').fillna(0)
                    df_clean['Type'] = df_clean['Yield'].apply(lambda x: 'Debt or related' if x != 0 else 'Equity or Equity related')
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_navi(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (in Lakhs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of Net Assets': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Handle both debt and equity instruments
                if 'Coupon' in df_clean.columns:
                    df_clean['Yield'] = pd.to_numeric(df_clean['Coupon'], errors='coerce').fillna(0)
                    df_clean['Type'] = df_clean['Yield'].apply(lambda x: 'Debt or related' if x != 0 else 'Equity or Equity related')
                else:
                    df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data

def clean_oldbridgecapital(df_raw, fund_names, sheets_to_avoid, AMC_NAME, datafile, output_file):
    full_data = pd.DataFrame()
    
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
                df_clean = df_clean.loc[:, df_clean.columns.notna()]

                # Standardize column names
                col_mapping = {
                    'Security Name': 'Name of Instrument',
                    'Industry/Rating': 'Industry',
                    'Market Value (Rs. in Lakhs)': 'Market Value',
                    'Market Value (In Lakhs)': 'Market Value',
                    '% to NAV': '% to Net Assets',
                    '% of Net Assets': '% to Net Assets'
                }
                df_clean = df_clean.rename(columns=col_mapping)
                df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)
                
                # Old Bridge Capital primarily deals with equity
                df_clean['Type'] = 'Equity or Equity related'
                
                df_clean = df_clean.round(2)
                df_clean["Scheme Name"] = fund
                df_clean["AMC"] = AMC_NAME
                full_data = pd.concat([full_data, df_clean], ignore_index=True) if not full_data.empty else df_clean

    full_data.to_excel(output_file, index=False)
    return full_data
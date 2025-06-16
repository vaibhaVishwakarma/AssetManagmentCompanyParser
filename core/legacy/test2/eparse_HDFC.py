from eparse.core import get_df_from_file, df_serialize_table
import re
import pandas as pd
import os

datadir='/Users/njp60/Documents/code/mutualfundbackend/funddata/data/HDFC Mutual Fund/'
output_file = 'hdfc_mutual_fund_porfolio.xlsx'
AMC_NAME="HDFC Mutual Fund"

#File=datadir+fundname
#df_generator = get_df_from_file(File)

def clean_fund_name(fund_name):
    # Use regex to keep everything before 'fund' (including 'fund') and remove everything after it
    cleaned_name = re.sub(r'\s+fund.*', ' fund', fund_name, flags=re.IGNORECASE)
    return cleaned_name.strip()

def is_valid_isin(value):
    """Check if a value is a valid ISIN."""
    if not isinstance(value, str):
        return False
    isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')
    return bool(isin_pattern.match(value))

def return_df_if_has_isin(df):
    """Return DataFrame if it contains at least one valid ISIN, else None."""
    if not isinstance(df, pd.DataFrame):
        return None
    
    # Flatten all values to a list of strings
    values = df.astype(str).values.flatten()
    
    # Check if any value in the DataFrame is a valid ISIN
    if any(is_valid_isin(v) for v in values):
        return df
    
    return None


def extract_fund_data(df_generator):
    """Extract fund data from a DataFrame generator."""
    for i, df in enumerate(df_generator):
        print(f"\nTable {i + 1}:")
        data=pd.DataFrame(df[0])

        if len(data.columns) >7 and return_df_if_has_isin(data) is not None:
            data.columns = data.iloc[0]
            data = data.drop(data.index[0]).reset_index(drop=True)
            data = data.dropna(subset=['Name Of the Instrument'])
            print(data.head(50))
            return data
    return None

def extract_fund_name(df_generator):
    """Extract fund data from a DataFrame generator."""
    for i, df in enumerate(df_generator):
        if i==0:
            fundname=pd.DataFrame(df[0])
            #print(f"\nFund Name: {fundname.iloc[0, 0]}")
            return clean_fund_name(fundname.iloc[0, 0])
    return None


def process_AMC(amc_directory, amc_name=AMC_NAME):

    full_data=pd.DataFrame()

    for root, _, files in os.walk(amc_directory):
        for file in files:
            if file.endswith((".xlsx", ".xls", ".xlsb")):
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                df_generator = get_df_from_file(file_path)
                fundname=extract_fund_name(df_generator)
                data=extract_fund_data(df_generator)
                if data is not None:
                    if fundname:
                        data["Scheme Name"]=fundname 
                    else:
                        data["Scheme Name"]="Unknown Fund/ETF"
                    data["AMC"]=amc_name
                    full_data=pd.concat([full_data,data],ignore_index=True) if not full_data.empty else data
                else:
                    print(f"No valid data found in {file_path}.")

    return full_data        

def save_to_excel(dataframe,output_file):
    """Save DataFrame to an Excel file."""
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False)
    print(f"Data saved to {output_file}")
    return 

data=process_AMC(datadir)
save_to_excel(data,output_file)
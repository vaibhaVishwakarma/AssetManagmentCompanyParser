import os 
import pandas as pd
import numpy as np
from fuzzywuzzy import process
directory = "final_cleaned"
files = list(filter(lambda x : "~" not in x , os.listdir(directory)))

df = pd.DataFrame()
for file in files:
    df2 = pd.read_excel(os.path.join(directory, file))
    print(len(df2))
    df = pd.concat([df,df2])

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
        match, score = process.extractOne(entry, keys)
        category = category_mapping.get(match, "others")
        results[entry] = category
    
    return results
mapped_entries = fuzzy_map_entries_to_category(np.unique(df["Type"]))
df["Type"] = df["Type"].map(mapped_entries)
df = df.drop("yield to call (ytc)", axis = 1)
new_cols = ["Name of Instrument",	"ISIN" , "Coupon" , "Industry",	"Quantity", "Market Value",	"% to Net Assets","Yield","Type","Scheme Name","AMC", "Scheme ISIN" ]
df.columns = new_cols
df.to_csv("Regular_JUNE_combined_sheet_reduced_types.csv",index = False)
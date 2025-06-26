from typing import override
from core.amcparser import AMCPortfolioParser
import pandas as pd
import re

class ICICIMFParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    @override
    def _get_fund_name(self, df):
        try:
            return list(filter(lambda x : "unnamed" not in x.lower() and "icici" in x.lower() , df.iloc[0,:].astype(str).to_list()))[0].strip()
        except Exception as e:
            return None

               
# Templates for all other AMC names
class One360Parser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class AdityaBirlaParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class AxisParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class BandhanParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df)<2 and len(df.columns) < 2 : return None
        return df.iloc[1,1]


class BankOfIndiaParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class BarodaBNPParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class CanaraRobecoParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class DSPParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class EdelweissParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self, df):
        """
        Extracts the fund name from the sheet DataFrame.
        This method can be overridden in subclasses for specific fund name extraction logic.
        """
        try: 
            name = df.columns[0].lower()
            name = re.sub(r"portfolio\s*\w*\s*of" , "" , name).strip()
            return name
        except Exception as e:
            print("⚠️ No fund name found in the sheet.")


class FranklinTempletonParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[0]


class GrowwParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    @override
    def _get_fund_name(self, sheet_df):
        try: 
            name = list(filter(lambda x : "unnamed" not in x.lower() and "groww" in x.lower() , list(sheet_df.columns)))[0].strip()
            name = re.sub(r"^IB\w*-","", name)
            return name
        except Exception as e:
            print("⚠️ No fund name found in the sheet.")
    


class HDFCParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    @override
    def _get_fund_name(self, sheet_df):
        # Default implementation: use the first non-empty cell in the first row
        name=sheet_df.head(0).columns[0]
        name= self._clean_fund_name(name)
        if name:
            return name
        else:
            print("⚠️ No fund name found in the sheet.")


class HeliosParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 or len(df) <4 : return None
        return df.iloc[1,3]
    


class HSBCParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 or len(df) <1 : return None
        return df.iloc[1,0]


class InvescoParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 or len(df) <2 : return None
        return df.iloc[1,1]
    
class OldBridgeCapital(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 or len(df) <2 : return None
        return df.iloc[1,1]


class ITIParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 or len(df) <2 : return None
        return df.iloc[1,1]


class JMFinancialParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 1 or len(df) <2 : return None
        return df.iloc[0,1]


class KotakParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)    

    @override
    def _get_fund_name(self, sheet_df):
        """
        Extracts the fund name from the sheet DataFrame.
        This method can be overridden in subclasses for specific fund name extraction logic.
        """
        # Default implementation: use the first non-empty cell in the first row
        try: 
            name = list(filter(lambda x : "unnamed" not in x.lower() and "kotak" in x.lower() , list(sheet_df.columns)))[0].split("as")[0].strip()
            name = name.split("Portfolio of")[1]
            name = re.sub(r"[^a-zA-z0.9\+\-\\\(\)\s]","",name)
            return name
        except Exception as e:
            print("⚠️ No fund name found in the sheet.")

class LICParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class MahindraManulifeParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 or len(df) <2 : return None
        return df.iloc[1,1]


class MiraeAssetParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        print(amc_config , default_config)
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self, df):
        try: 
            return df.columns[1].strip()
        except Exception as e:
            print("⚠️ No fund name found in the sheet.")
    


class MotilalOswalParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class NaviParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df)<4 and len(df.columns) < 1  : return None
        return df.iloc[3,0]


class NipponIndiaParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class NJParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df)<1 and len(df.columns) < 2  : return None
        return df.iloc[0,1]


class PGIMIndiaParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class PPFASParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    @override
    def _get_fund_name(self, df):
        name=list(filter(lambda x : "unnamed" not in x.lower(), df.columns.astype(str)))[0].strip()
        name= self._clean_fund_name(name)
        if name:
            return name
        else:
            print("⚠️ No fund name found in the sheet.")


class QuantParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        return df.iloc[0,2].strip()


class QuantumParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df)<6 and len(df.columns) < 1  : return None
        return df.iloc[5,0]

class SBIParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 4 : return None
        return df.iloc[1,3]
        


class ShriramParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class SundaramParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df)<1 and len(df.columns) < 1  : return None
        return df.iloc[0,0]


class TataParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class TrustParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df)<3 and len(df.columns) < 1  : return None
        return df.iloc[2,0].split("\n")[0]


class UnionParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df)<6 and len(df.columns) < 3  : return None
        return df.iloc[5,2].lower().split("of")[1].split("fund")[0]


class UTIParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for UTI Mutual Fund
        pass


class WhiteOakParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]


class ZerodhaParser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config):
        super().__init__(amc_config=amc_config, default_config=default_config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Zerodha Fund House
        pass










from core.amcparser import AMCPortfolioParser
import pandas as pd

class ICICIMFParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):

        print(f"\n🔍 Processing  → Sheet: {sheet_name}")

        fund = self.fund_name_extraction_logic(sheet_df)

        if fund is None:
            print(f"⚠️ Skipping {sheet_name} (Could not extract fund name)")
            return

        print(f"\n🔍 Processing  → Fund: {fund}")

        header_row_idx = next(
            (index for index, row in sheet_df.iterrows() if any("ISIN" in str(val) for val in row.dropna())),
            None
        )
        if header_row_idx is None:
            print(f"⚠️ Skipping {sheet_name} (No ISIN header found)")
            return

        # Read the Excel file, skipping rows up to the header, and explicitly setting header=None
        df_clean = pd.read_excel(datafile, sheet_name=sheet_name, skiprows=header_row_idx, header=None, dtype=str)

        # Set the first row of the new DataFrame as the header
        df_clean.columns = df_clean.iloc[0]

        # Remove the header row from the data
        df_clean = df_clean[1:].reset_index(drop=True)

        df_clean = df_clean.loc[:, df_clean.columns.notna()] # Remove unnamed columns

        print("Columns after initial read and unnamed column removal:", df_clean.columns.tolist()) # Debug print

        # Apply column mapping
        df_clean.rename(columns=self.column_mapping, inplace=True)

        print("Columns after renaming:", df_clean.columns.tolist()) # Debug print

        # Ensure 'Coupon' column exists and is numeric
        if "Coupon" not in df_clean.columns:
            df_clean["Coupon"] = 0
        else:
            df_clean["Coupon"] = pd.to_numeric(df_clean["Coupon"], errors='coerce').fillna(0)

        # Filter out rows with missing essential data
        df_clean.dropna(subset=["ISIN", "Name of Instrument", "Market Value"], inplace=True)

        # Apply instrument type logic
        df_clean = self.instrument_type_logic(df_clean)

        df_clean = df_clean.round(2)
        df_clean["Scheme Name"] = fund
        df_clean["AMC"] = self.amc_name

        # Reorder and select final columns if specified
        if self.final_columns:
            # Ensure all final_columns are present, fill missing with NaN
            for col in self.final_columns:
                if col not in df_clean.columns:
                    df_clean[col] = None
            df_clean = df_clean[self.final_columns]

        self.full_data = pd.concat([self.full_data, df_clean], ignore_index=True) if not self.full_data.empty else df_clean


# Templates for all other AMC names
class One360Parser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for 360 One Asset Management
        pass


class AdityaBirlaParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Aditya Birla Sun Life Mutual Fund
        pass


class AxisParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Axis Mutual Fund
        pass


class BandhanParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Bandhan Mutual Fund
        pass


class BankOfIndiaParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Bank of India Mutual Fund
        pass


class BarodaBNPParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Baroda BNP Paribas Mutual Fund
        pass


class CanaraRobecoParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Canara Robeco Mutual Fund
        pass


class DSPParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for DSP Mutual Fund
        pass


class EdelweissParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Edelweiss Mutual Fund
        pass


class FranklinTempletonParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Franklin Templeton India
        pass


class GrowwParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Groww Mutual Fund
        pass


class HDFCParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for HDFC Mutual Fund
        pass


class HeliosParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Helios Mutual Fund
        pass


class HSBCParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for HSBC Mutual Fund
        pass


class InvescoParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Invesco Mutual Fund
        pass


class ITIParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for ITI Mutual Fund
        pass


class JMFinancialParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for JM Financial Mutual Fund
        pass


class KotakParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Kotak Mutual Fund
        pass


class LICParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for LIC Mutual Fund
        pass


class MahindraManulifeParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Mahindra Manulife Mutual Fund
        pass


class MiraeAssetParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Mirae Asset Mutual Fund
        pass


class MotilalOswalParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Motilal Oswal Mutual Fund
        pass


class NaviParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Navi Mutual Fund
        pass


class NipponIndiaParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Nippon India Mutual Fund
        pass


class NJParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for NJ Mutual Fund
        pass


class PGIMIndiaParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for PGIM India Mutual Fund
        pass


class PPFASParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for PPFAS Mutual Fund
        pass


class QuantParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Quant Mutual Fund
        pass


class QuantumParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Quantum Mutual Fund
        pass


class SBIParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for SBI Mutual Fund
        pass


class ShriramParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Shriram Mutual Fund
        pass


class SundaramParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Sundaram Mutual Fund
        pass


class TataParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Tata Mutual Fund
        pass


class TrustParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Trust Mutual Fund
        pass


class UnionParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Union Mutual Fund
        pass


class UTIParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for UTI Mutual Fund
        pass


class WhiteOakParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for WhiteOak Mutual Fund
        pass


class ZerodhaParser(AMCPortfolioParser):
    def __init__(self, config):
        super().__init__(config=config)

    def process_sheet(self, datafile, sheet_name, sheet_df):
        # TODO: Implement the specific cleaning logic for Zerodha Fund House
        pass










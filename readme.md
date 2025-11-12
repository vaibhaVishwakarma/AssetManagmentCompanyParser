[Install Requirements](requirements.txt)
```bash
pip install -r requirements.txt
```

[Main](config/amc_configs.yaml) 
```bash
python -m main
```

[Example Config File](config/amc_configs.yaml)  
```yaml 
 Defaults: &default
  path_BSE_SchemeData: ./reference/SchemeData0111251523SS.csv
  final Columns: &default_columns
  - Name of Instrument
  - ISIN
  - Coupon
  - Industry / Rating
  - Quantity
  - Market Value (MKT) ( rs lakh )
  - '% to Net Assets (NAV)'
  - Yield
  - Yield to call (YTC)
 
 360 One Asset Management:
  AMCName: 360 One Asset Management
  DataDirectory: ./data/360 One Asset Management
  sheets_to_avoid: []
  final_columns: *default_columns
  Scale100: ""
  Scale100th: ""
```


#### Debuggers Guide  

[Main Functionality](main.py)

```python
amc_parser_mapping = {
    "360 One Asset Management": One360Parser,
    }  

for amc_name, parser_cls in amc_parser_mapping.items():  
    amc_cfg = configs.get(amc_name, {})    
    parser = parser_cls(amc_cfg, default_config, embedding_model)  
    parser.parse_all_portfolios()  
    parser.save_to_excel()  

post = PortfolioPostProcessor()  
post.clean_data()  
post.compile_final_output()  
```


[Core Parser](core/amcparser.py)
```python
class AMCPortfolioParser(ABC):
    # Orchestration Methods
    def parse_all_portfolios(self):...
        self._read_excel_file(file_path)
        self.process_sheet(file_path, sheet_name, df)
    def save_to_excel(self):...

    # Core Parsing Logic
    def process_sheet(self, file_path, sheet_name, df):...
        self._get_fund_name(df)

        # header normalization
        header_row = self._fetch_header_row(df)
        # indicies with entries
        periods = self._get_valid_periods(df, header_map)
        # ... Append Row ...
```
[AMC Custom Class](core/parser.py)
```python
# Type 1
class One360Parser(AMCPortfolioParser):
    def __init__(self, amc_config, default_config, embedding_model):
        super().__init__(amc_config=amc_config, default_config=default_config, embedding_model = embedding_model)

    def _get_fund_name(self,df):
        if len(df.columns) < 2 : return None
        return df.columns[1]
# Type 2
class BandhanParser(AMCPortfolioParser):
    # ... __init__ ...

    def _get_fund_name(self,df):
        if len(df)<2 and len(df.columns) < 2 : return None
        return df.iloc[0,1]    
```

[Post Processing Class](core/postprocessor.py)
```python
class PortfolioPostProcessor:   
    # Step 1: Data Cleaning
    def clean_data(self):...

    # Step 2: Final Compilation
    def compile_final_output(self):...

    # Step 3: Scheme ISIN and Amfi Code addition  
    def _join_bse_schemedata(self, portfolio_df: pd.DataFrame):...  

```

---

#### File Structure Guide
\AssetManagmentCompanyParser    
│  
├── Portfolio_extracted_121125-2125.csv   # OUTPUT file    
│  
├── main.py  
│  
├── core\    
│ ├── amcparser.py  
│ ├── parser.py  
│ └── postprocessor.py  
│  
├────────────────────────────────────────────  
│  
├── config\    
│ └── amc_configs.yaml  
│  
├── reference\    
│ └── SchemeData.csv  
│  
├── data\  
│ ├── 360 One Asset Management\    
│ │ └── IN_MF_MONTHLY_PORTFOLIO-Sept2025.xls  
│ │  
│ └── Aditya Birla Sun Life Mutual Fund\    
│ │ └── SEBI_Monthly_Portfolio 30 SEP 2025.xls  
│  
├────────────────────────────────────────────  
│  
├── .cleaned\                # DEBUGGING only    
└── .final_cleaned\          # DEBUGGING only    
│  
├── parser.log              # DEBUGGING only    
├── testscheme.ipynb        # DEBUGGING only    
│   
├────────────────────────────────────────────  
│   
├── readme.md  
├── requirements.txt  
├── .gitignore  
  

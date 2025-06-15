olioParser(config_json[AMC])
    print(f"---- Processing {AMC} ---- ")
    try:
        parser.run()
        fund_name = amc_to_fund_map[AMC]
        parser.append("main_output.xlsx", fund_name  ,  isin_mapping[fund_name] )
    except Exception as e:
        print(e)
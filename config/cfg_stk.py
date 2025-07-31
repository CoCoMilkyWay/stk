class cfg_stk:  # Constants Configs
    import os

    period_u, period_l, n = 'min', 'm', 1
    wt_period_u = period_u + str(n)
    wt_period_l = period_l + str(n)

    start = 202401010000 # at least 1 asset has data
    end = 202402010000 # can be in future
    capital = 10000000

    wt_tradedays = 'CHINA'
    wt_session = 'SD0930'

    # main functions:
    profile = False
    plot = False

    exchg = ['SSE', 'SZSE', 'BJSE']
    product = 'STK'

    script_dir = os.path.dirname(os.path.abspath(__file__))
    WT_DATABASE_DIR = os.path.abspath(script_dir + '/../../../database')  # raw data
    WT_STORAGE_DIR = os.path.abspath(script_dir + '/../../storage')  # processed data

    if os.name == 'posix':  # Linux
        STOCK_CSV_DIR = os.path.expanduser("~/work/data/stock_csv")
        STOCK_DB_BAR_DIR = WT_DATABASE_DIR + '/stock/bars'
        STOCK_DB_FUND_DIR = WT_DATABASE_DIR + '/stock/fundamentals'
    else: # Windows
        STOCK_CSV_DIR = 'E:/raw_m1/stk'
        # STOCK_CSV_DIR = 'D:/data/stock_csv'
        STOCK_DB_BAR_DIR = 'E:/stock_db/bars'
        # STOCK_DB_BAR_DIR = 'D:/data/stock_db/bars'
        STOCK_DB_FUND_DIR = 'D:/data/stock_fundamental'

    # config files
    lxr_asset_file = script_dir + '/lxr_assets.json'
    wt_asset_file = script_dir + '/stk_assets.json'
    lxr_profile_file = script_dir + '/info/lxr_profile.json'
    lxr_industry_file = script_dir + '/info/lxr_industry.json'
    wt_adj_factor_file = script_dir + '/stk_adjfactors.json'
    wt_tradedays_file = script_dir + '/stk_tradedays.json'
    wt_holidays_file = script_dir + '/stk_holidays.json'
    
    # MISC =========================================
    FEE = 0.0015
    # memory management (manual set for simplicity)
    max_trade_session_ratio = 250/365*(4.5/24)/n

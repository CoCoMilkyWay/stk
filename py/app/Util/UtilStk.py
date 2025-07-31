from config.cfg_stk import cfg_stk
import os
import json
import time
import copy
import tempfile
import numpy as np
import pandas as pd

from tqdm import tqdm
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional

from DataProvider_API.Lixingren.LixingrenAPI import LixingrenAPI
from DataProvider_API.Baostock.BaostockAPI import BaostockAPI


RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
PURPLE = '\033[95m'
CYAN = '\033[96m'
DEFAULT = '\033[0m'

SEC_IN_HALF_YEAR = int(3600*24*365*0.5)


def load_json(file_path):
    import json
    # Note the 'utf-8-sig' which handles BOM if present
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        return json.load(file)


def dump_json(file_path, df, desc=None):
    if desc:
        print(f'{desc+':':20}{GREEN}{file_path}{DEFAULT} ... ', end="")
    # Create a temporary file in the same directory as the target file.
    dir_name = os.path.dirname(file_path)
    with tempfile.NamedTemporaryFile('w', delete=False, dir=dir_name, encoding='utf-8', errors='ignore') as temp_file:
        # Write the JSON data to the temporary file.
        json.dump(df, temp_file, indent=4, ensure_ascii=False)
        # Flush the file and force the OS to write to disk.
        temp_file.flush()
        os.fsync(temp_file.fileno())
        temp_path = temp_file.name

    while True:
        if os.path.exists(temp_path):
            break
        time.sleep(0.05)

    # Atomically replace the target file with the temporary file.
    os.replace(temp_file.name, file_path)
    if desc:
        print(f'dumped')


def store_sparse_df(df: pd.DataFrame, path: str):
    # df has a time index and many columns with leading NaNs
    df.astype(pd.SparseDtype("float64", fill_value=float("nan"))
              ).to_parquet(path, index=True)


def load_sparse_df(path: str):
    return pd.read_parquet(path).sparse.to_dense()


def store_compressed_array(arr, path):
    np.savez_compressed(mkdir(path), data=arr)


def load_compressed_array(path):
    with np.load(path) as data:
        return data['data']


def mkdir(path_str):
    path = os.path.dirname(path_str)
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    return path_str

# ================================================
# prepare all files
# ================================================


LXR_API = LixingrenAPI()
BS_API = BaostockAPI()


def prepare_all_files(filter_list: Optional[List[str]] = None):
    """
    wt: wondertrader/wtpy
    lxr: lixingren
    """

    # 问题:
    # 1. stk_assets.json 一般是静态, 但变动时不会自动更新(比如退市)(但是更新日期有记录)
    # 1. stk_adjfactors.json 可能没有更新(但是更新日期有记录)
    # 2. fundamental meta 可以被更新. 并且精确根据年内天数创建numpy array, 但是老的数据不会被更新

    wt_asset = _wt_asset_file(cfg_stk.wt_asset_file)
    lxr_profile, wt_asset = _lxr_profile_file(cfg_stk.lxr_profile_file, wt_asset)
    lxr_industry, wt_asset = _lxr_industry_file(cfg_stk.lxr_industry_file, wt_asset)
    wt_adj_factor, wt_asset = _wt_adj_factor_file(cfg_stk.wt_adj_factor_file, wt_asset)
    wt_tradedays, wt_holidays = _wt_tradedays_holidays_file(cfg_stk.wt_tradedays_file, cfg_stk.wt_holidays_file)

    dump_json(cfg_stk.wt_asset_file, wt_asset, "wt_asset")
    dump_json(cfg_stk.lxr_profile_file, lxr_profile, "lxr_profile")
    dump_json(cfg_stk.lxr_industry_file, lxr_industry, "lxr_industry")
    dump_json(cfg_stk.wt_adj_factor_file, wt_adj_factor, "wt_adj_factor")
    dump_json(cfg_stk.wt_tradedays_file, wt_tradedays, "wt_tradedays")
    dump_json(cfg_stk.wt_holidays_file, wt_holidays, "wt_holidays")

    # lxr_meta = _lxr_fundamental_file(cfg_stk.STOCK_DB_FUND_DIR, wt_asset, wt_tradedays)
    # dump_json(f"{cfg_stk.STOCK_DB_FUND_DIR}/meta.json", lxr_meta, 'lxr_index_meta')

    if filter_list:
        filtered_wt_asset = {}
        for exg in wt_asset:
            filtered_wt_asset[exg] = {}
            for key in wt_asset[exg]:
                if wt_asset[exg][key]['code'] in filter_list:
                    filtered_wt_asset[exg][key] = wt_asset[exg][key]
        wt_asset = filtered_wt_asset

    return wt_asset


def _wt_asset_file(path: str) -> Dict:

    Status = {
        "normally_listed": {
            "name": "正常上市",
            "description": "指公司目前在证券交易所上市，并正常交易，没有任何限制。"
        },
        "delisted": {
            "name": "已退市",
            "description": "指公司的股票已从交易所撤销，不能再进行交易。这可能由于多种原因，包括财务不稳定或未能满足上市要求。"
        },
        "listing_suspended": {
            "name": "暂停上市",
            "description": "意味着公司的股票交易已被暂时暂停，可能由于正在进行的调查、财务困难或其他需要解决的问题。"
        },
        "special_treatment": {
            "name": "ST板块",
            "description": "该类别的股票因财务困难或其他重大风险而受到特别处理，通常会受到更严格的监管和监控。"
        },
        "delisting_risk_warning": {
            "name": "*ST",
            "description": "该标识表示公司因财务状况或其他严重问题而面临退市风险，提醒投资者可能会被退市的风险。"
        },
        "issued_but_not_listed": {
            "name": "已发行未上市",
            "description": "指已发行但目前未在任何证券交易所上市的证券，因此无法进行公开交易。"
        },
        "pre_disclosure": {
            "name": "预披露",
            "description": "该状态表示公司计划上市，并已就其意图进行初步披露，通常是在首次公开募股（IPO）之前。"
        },
        "unauthorized": {
            "name": "未过会",
            "description": "表示公司尚未获得监管机构的必要批准以进行上市或公开发行其股票。"
        },
        "issue_failure": {
            "name": "发行失败",
            "description": "该术语指公司未能成功发行证券，通常意味着没有吸引到足够的投资者兴趣。"
        },
        "delisting_transitional_period": {
            "name": "进入退市整理期",
            "description": "该状态表示公司在正式退市之前的一个阶段，此期间可能还会继续交易，但会受到密切监控。"
        },
        "ipo_suspension": {
            "name": "暂缓发行",
            "description": "意味着公司的首次公开募股（IPO）计划已被暂时暂停，可能由于监管问题或市场状况。"
        },
        "ipo_listing_suspension": {
            "name": "暂缓上市",
            "description": "类似于上面，表示某项证券的上市已被推迟。"
        },
        "transfer_suspended": {
            "name": "停止转让",
            "description": "表示股票的所有权转让已被暂停，这可能是由于监管问题或其他复杂情况。"
        },
        "normally_transferred": {
            "name": "正常转让",
            "description": "指股票在没有任何限制或特殊情况的情况下正常进行转让。"
        },
        "investor_suitability_management_implemented": {
            "name": "投资者适当性管理标识",
            "description": "该标识表示公司正在实施投资者适当性管理措施，确保其投资产品适合目标投资者群体。"
        },
        "non_listed": {
            "name": "非上市",
            "description": "该术语描述的是未在任何证券交易所上市的证券，因此不进行公开交易。"
        },
        "transfer_as_specific_bond": {
            "name": "特定债券转让",
            "description": "指在特定条款和条件下转让某些债券，通常不在常规交易框架内进行。"
        },
        "transfer_under_agreement": {
            "name": "协议转让",
            "description": "表示所有权的转让是基于双方之间的协议进行的，而不是通过公共交易过程。"
        },
        "others": {
            "name": "其它",
            "description": "这是一个涵盖未在其他定义中列出的任何上市状态的通用类别。"
        }
    }

    tradable = {
        'normally_listed',
        'special_treatment',
        'delisting_risk_warning',
        'delisted',
    }

    def get_sub_exchange(code):
        # https://www.cnstock.com/image/202311/24/20231124175032784.pdf
        if code.startswith('600') or code.startswith('601') or code.startswith('603') or code.startswith('605'):  # 沪市主板
            return 'SSE.1.A'
        elif code.startswith('900'):  # 沪市B股
            return 'SSE.1.B'
        elif code.startswith('688'):  # 科创板 (普通+成长层) (Sci-Tech innovAtion boaRd) (2019/03)
            return 'SSE.2.STAR'
        elif code.startswith('689'):  # 科创板存托凭证 (Depository Receipt)
            return 'SSE.2.STAR.CDR'
        # https://github.com/jincheng9/finance_tutorial/blob/main/workspace/securities/szse_securities_code_allocation_2024.pdf
        elif code.startswith('000'):  # 深市主板
            return 'SZSE.1.A'
        elif code.startswith('001'):  # 深市主板 或 存托凭证(001001-001199)
            return 'SZSE.1.A'
        elif code.startswith('002') or code.startswith('003') or code.startswith('004'):  # 深市中小板 (SME Board) (2004/06)
            return 'SZSE.1.A.SME'
        elif code.startswith('200') or code.startswith('201'):  # 深市B股
            return 'SZSE.1.B'
        elif code.startswith('300') or code.startswith('301') or code.startswith('302'):  # 创业板 (ChiNext) (2009/09)
            return 'SZSE.2.CN'
        elif code.startswith('309'):  # 创业板存托凭证
            return 'SZSE.2.CN.CDR'
        # https://www.bseinfo.net/uploads/6/file/public/202404/20240419121551_vhsa4vwi0q.pdf
        # 新三板(National Equities Exchange and Quotations) (2013/12)
        elif code.startswith('400'):  # 两网公司(STAQ、NET)及退市公司A股
            return 'NEEQ.3.DL_A'
        elif code.startswith('420'):  # 退市B股
            return 'NEEQ.3.DL_B'
        elif code.startswith('430'):  # 新三板.基础层 (< 2014/05/19)
            return 'NEEQ.5.FND'
        elif code.startswith('82'):  # 新三板.创新层.优先股(Preferred) (> 2014/05/19)
            return 'NEEQ.5.INV.PREF'
        elif code.startswith('83'):  # 新三板.创新层.普通股(Ordinary) (> 2014/05/19)
            return 'NEEQ.5.INV.ORDI'
        elif code.startswith('87'):  # 新三板.精选层(北交所).普通发行(私募/协议转让/定向增发) (> 2020/07/27)
            return 'BJSE.3.SEL.ORDI'
        elif code.startswith('88'):  # 新三板.精选层(北交所).公开发行 (> 2020/07/27)
            return 'BJSE.3.SEL.PUBLIC'
        elif code.startswith('92'):  # 北交所
            return 'BJSE.3'
        else:
            print('Unknown sub-exchange: ', code)
            return 'Unknown'

    state = _check_state(path, "WT-AssetInfo", 1)

    if state != 0:
        old = load_json(path)
    else:  # non-exist
        old = {}

    state = _check_state(path, "LXR-AssetInfo", 1)
    raw_path = cfg_stk.lxr_asset_file
    if state == 2: # new
        new = load_json(raw_path)
    else:  # non-exist
        print('HTTP Querying A-stock ExchangeInfo...')
        new = LXR_API.query("basic_all")
        dump_json(raw_path, new)

    output = {"SSE": {}, "SZSE": {}, "BJSE": {}, }
    simple = copy.deepcopy(output)
    map = {'bj': 'BJSE', 'sh': 'SSE', 'sz': 'SZSE'}

    # Populate the data for each symbol
    for symbol in new:
        name = symbol.get('name')
        market = symbol.get('market')
        exchange = symbol.get('exchange')
        areaCode = symbol.get('areaCode')
        stockCode = symbol.get('stockCode')
        fsTableType = symbol.get('fsTableType')
        ipoDate = symbol.get('ipoDate')
        delistedDate = symbol.get('delistedDate')
        listingStatus = symbol.get('listingStatus')
        mutualMarkets = symbol.get('mutualMarkets')
        if not (name and market and exchange and areaCode and stockCode and fsTableType and ipoDate and listingStatus):
            print('Skipping for incomplete info: ', name, listingStatus)
            continue

        if listingStatus not in tradable:
            print('Skipping for listingStatus:', name,
                  Status[listingStatus]['description'])
            continue

        if areaCode not in ['cn']:
            print('Skipping for areaCode:', name, areaCode)
            continue

        # if market not in ['a']:
        #     print('Skipping for market:', name, market)
        #     continue

        if exchange not in ['sh', 'sz', 'bj']:
            print('Skipping for exchange:', name, exchange)
            continue

        # if fsTableType not in ['non_financial']:
        #     print('Skipping for fsTableType:', name, fsTableType)
        #     continue

        # ipo_date_s = datetime.fromisoformat(ipoDate).timestamp()
        # if abs(int(time.time()) - ipo_date_s) <= SEC_IN_HALF_YEAR:
        #     print('Skipping for recency(half year):', name)
        #     continue

        exg = map[exchange]
        output[exg][stockCode] = {
            "code": stockCode,
            "exchg": exg,
            "name": name,
            "product": "STK",
            "extras": {
                "ipoDate": ipoDate,
                "delistedDate": delistedDate,
                "subexchg": get_sub_exchange(stockCode),
                "industry_names": None,
                "industry_codes": None,
                "companyName": None,
                "website": None,
                "city": None,
                "province": None,
                "businessScope": None,
                # 'ha': 港股通
                "mutualMarkets": mutualMarkets if mutualMarkets else [],
                "fsTableType": fsTableType,

                "update_time_profile": None,
                "update_time_adjfactor": _get_nested_value(old, [exg, stockCode, 'extras', 'update_time_adjfactor']),
                "update_time_fundamental": _get_nested_value(old, [exg, stockCode, 'extras', 'update_time_fundamental']),
            },
        }

        simple[exg][stockCode] = {
            "code": stockCode,
            "exchg": exg,
            "name": name,
            "product": "STK",
        }
    dump_json(f"{cfg_stk.script_dir+'/stk_assets_simple.json'}",
              simple, "wt_asset")
    return output


def _lxr_profile_file(path: str, wt_asset: Dict):
    state = _check_state(path, "LXR-Profile", 1)
    exgs = cfg_stk.exchg
    API_LIMITS = 100

    if state != 0:  # exist
        old_lxr = load_json(path)
    else:
        old_lxr = {}
        for exg in exgs:
            old_lxr[exg] = {}

    time = datetime.now(timezone(timedelta(hours=8))).isoformat()  # East Asia

    lxr = {}
    for exg in exgs:
        lxr[exg] = {}
        pending_assets = []
        for key in wt_asset[exg]:
            if key in old_lxr[exg]:
                lxr[exg][key] = old_lxr[exg][key]
            else:
                lxr[exg][key] = None
                pending_assets.append(key)

        pending_assets_lists = _split_list(pending_assets, API_LIMITS)

        if len(pending_assets) != 0:
            print(f"Updating {len(pending_assets)} profiles for: {exg}")
            for pending_assets_list in tqdm(pending_assets_lists):
                assets = LXR_API.query("profile", pending_assets_list)
                assert len(assets) == len(pending_assets_list)
                for asset in assets:
                    code = asset['stockCode']
                    assert code in pending_assets_list
                    lxr[exg][code] = asset
                    lxr[exg][code]['name'] = wt_asset[exg][code]['name']
                    lxr[exg][code]['update_time_profile'] = time

        for key in wt_asset[exg]:
            wt_ = wt_asset[exg][key]['extras']
            lxr_ = lxr[exg][key]
            wt_['update_time_profile'] = lxr_.get('update_time_profile')
            wt_['companyName'] = lxr_.get('companyName')
            wt_['website'] = lxr_.get('website')
            wt_['city'] = lxr_.get('city')
            wt_['province'] = lxr_.get('province')
            wt_['businessScope'] = lxr_.get('businessScope')

    return lxr, wt_asset


def _lxr_industry_file(path: str, wt_asset: Dict):
    state = _check_state(path, "LXR-Industry", 1)
    exgs = cfg_stk.exchg

    if state != 0:  # exist
        old_lxr = load_json(path)
    else:
        old_lxr = {}
        for exg in exgs:
            old_lxr[exg] = {}

    sw21 = _parse_industry()

    lxr = {}
    for exg in exgs:
        lxr[exg] = {}
        pending_assets = []
        for key in wt_asset[exg]:
            # if len(lxr[exg].keys()) > 10: break
            if key in old_lxr[exg]:
                lxr[exg][key] = old_lxr[exg][key]
            else:
                lxr[exg][key] = None
                pending_assets.append(key)

        if len(pending_assets) != 0:
            print(f"Updating {len(pending_assets)} industry for: {exg}")
            for pending_asset in tqdm(pending_assets):
                assets = LXR_API.query("industries", pending_asset)
                lxr[exg][pending_asset] = assets

    lvl = {'one': 0, 'two': 1, 'three': 2}

    for exg in exgs:
        for key in wt_asset[exg]:
            codes = ["", "", ""]
            names = ["", "", ""]
            for item in lxr[exg][key]:
                if item["source"] == "sw_2021":
                    code = item["stockCode"]
                    level, name = sw21[code]
                    level = lvl[level]
                    codes[level] = code
                    names[level] = name
            if "" in codes or "" in names:
                print(f"Err updating industries for {key}:{wt_asset[exg][key]["name"]}-{wt_asset[exg][key]["extras"]["subexchg"]}")
            wt_asset[exg][key]['extras']['industry_codes'] = codes
            wt_asset[exg][key]['extras']['industry_names'] = names

    return lxr, wt_asset


def _lxr_dividend_file(path: str, wt_asset: Dict):
    """ 分红
    - board_director_plan (董事会预案): Proposed by the board of directors.                                    
    - shareholders_meeting_plan (股东大会预案): Submitted for approval at the shareholders' meeting.           
    - company_plan (公司预案): Internally proposed dividend plan by the company.                               
    - delay_implementation (延迟实施): Dividend payout implementation is delayed.                              
    - cancelled (取消分红): Dividend distribution has been cancelled.                                          
    - implemented (已执行): Dividend plan has been fully executed.                                             
    - terminated (终止): Dividend process has been terminated.                                                 
    - plan (预案): General status indicating that a dividend plan is in place, though it may not be finalized. 

    NOTE: shareholders_meeting(high importance) supervise over the board
          board_director_plan(medium importance) has to be submitted to and reviewed by shareholders_meeting
          company_plan(low importance) may or may not be submitted to shareholders_meeting
          dividends are usually board_director_plan, has to be approved by shareholders_meeting
          some major decisions are done by shareholders_meeting directly (major investment, profit allocations etc.)

    NOTE:
    - adj_factor = adj_dividend * adj_allotment
    - adj_dividend = (1-Cash_Dividend_per_Share/Previous_Close_Price_per_Share)
    - adj_allotment = (1/(1+Bonus_Shares_per_Share))
    """
    return None, None


def _lxr_allotment_file(path: str, wt_asset: Dict):
    """ 配股
    - board_directors_approved (董事会通过): Dividend plan has been approved by the board of directors.
    - shareholders_meeting_approved (股东大会通过): Dividend plan has been approved by the shareholders' meeting.
    - approved (已批准): Dividend plan has received approval.
    - implemented (已执行): Dividend plan has been fully executed.
    - postphoned (已延期): Dividend plan implementation has been postponed.
    - terminated (终止): Dividend process has been terminated.
    - unapproval (未获准): Dividend plan has not been approved.

    NOTE:
    - adj_factor = adj_dividend * adj_allotment
    - adj_dividend = (1-Cash_Dividend_per_Share/Previous_Close_Price_per_Share)
    - adj_allotment = (1/(1+Bonus_Shares_per_Share))
    """
    return None, None


def _wt_adj_factor_file(path: str, wt_asset: Dict):
    state = _check_state(path, "WT-AdjFactor", 1)
    exgs = cfg_stk.exchg
    map = {'SSE': 'sh', 'SZSE': 'sz', 'BJSE': 'bj', }

    if state != 0:  # exist
        old_wt = load_json(path)
    else:
        old_wt = {}
        for exg in exgs:
            old_wt[exg] = {}

    time = datetime.now(timezone(timedelta(hours=8))).isoformat()  # East Asia

    wt = {}
    for exg in exgs:
        wt[exg] = {}
        pending_assets = []
        for key in wt_asset[exg]:
            if key in old_wt[exg] and wt_asset[exg][key]['extras']['update_time_adjfactor'] is not None:
                wt[exg][key] = old_wt[exg][key]
            else:
                wt[exg][key] = None
                pending_assets.append(key)
                wt_asset[exg][key]['extras']['update_time_adjfactor'] = time

        if len(pending_assets) != 0:
            print(f"Updating {len(pending_assets)} adj_factors for: {exg}")
            codes = [f"{map[exg]}.{asset}" for asset in pending_assets]
            adj_factors = BS_API.query_adjust_factor(
                codes, '1990-01-01', '2050-01-01')
            for pending_asset in pending_assets:
                wt[exg][pending_asset] = adj_factors[pending_asset]
    return wt, wt_asset


def _wt_tradedays_holidays_file(tradedays_path: str, holidays_path: str):
    state_h = _check_state(holidays_path, "WT-Holidays", 1)
    state_t = _check_state(tradedays_path, "WT-Tradedays", 1)
    if state_t == 2 and state_h == 2:
        return load_json(tradedays_path), load_json(holidays_path)

    import akshare as ak
    tradedays_df = ak.tool_trade_date_hist_sina()
    # Convert trade_date column to datetime
    tradedays_df['trade_date'] = pd.to_datetime(tradedays_df['trade_date'])
    # Generate the complete range of weekdays
    start_date = tradedays_df['trade_date'].min()
    end_date = tradedays_df['trade_date'].max()
    all_weekdays = pd.date_range(start=start_date, end=end_date, freq='B')
    # Convert the trade dates to a set for faster operations
    trade_dates_set = set(tradedays_df['trade_date'])
    # Filter out the trade dates to find holidays
    tradedays = sorted([date for date in trade_dates_set])
    holidays = sorted(
        [date for date in all_weekdays if date not in trade_dates_set])
    # Convert holidays list to a DataFrame
    tradedays_df = pd.DataFrame({'CHINA': tradedays})
    tradedays_df['CHINA'] = tradedays_df['CHINA'].dt.strftime('%Y%m%d')
    holidays_df = pd.DataFrame({'CHINA': holidays})
    holidays_df['CHINA'] = holidays_df['CHINA'].dt.strftime('%Y%m%d')
    # Create a JSON object with "CHINA" as the key and the formatted dates as a list
    tradedays_json = {"CHINA": tradedays_df['CHINA'].tolist()}
    holidays_json = {"CHINA": holidays_df['CHINA'].tolist()}
    return tradedays_json, holidays_json


def _get_nested_value(d, keys, default=None):
    for key in keys:
        d = d.get(key, default)
        if d == default:
            return default
    return d


def _parse_industry() -> Dict:
    sw21_file = cfg_stk.script_dir + '/info/shenwan2021.json'
    sw21_list = load_json(sw21_file)

    sw21 = {}
    for item in sw21_list:
        # if "delistedDate" not in item.keys():
        code = item["stockCode"]
        level = item["level"]
        name = item["name"]
        sw21[code] = (level, name)
    return sw21


def _split_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def _generate_dt_ranges(start_year: int, end_year: int, interval: int = 10):
    ranges = []
    for year in range(start_year, end_year, interval):
        start_date = f"{year}-01-01"
        end_date = f"{year + interval-1}-12-31"
        ranges.append((start_date, end_date))
    return ranges


def _check_state(file_path: str, file_name: str, days: int = 1) -> int:
    """
    not exist: 0,
    old: 1,
    new: 2,
    """

    if not os.path.exists(file_path):
        print(f'{file_name} not exist')
        return 0

    timestamp_last_update_s = os.path.getmtime(file_path)
    dt = datetime.fromtimestamp(timestamp_last_update_s)
    updated_within_x_day = \
        abs(time.time() - timestamp_last_update_s) <= (3600*24*days)
    if updated_within_x_day:
        print(f'{file_name} Already Updated: {dt.year}-{dt.month}-{dt.day}')
        return 2
    else:
        print(f'Old {file_name}: {dt.year}-{dt.month}-{dt.day}')
        return 1


# ================================================
# Fundamental DataBase
# ================================================


def _lxr_fundamental_file(path: str, wt_asset: Dict, wt_tradedays: Dict):
    # print('Analyzing/Generating Fundamental database files...')
    print(f"Fundamental_Npz:    {GREEN}{path}/<symbol>/{DEFAULT}")

    API_LIMITS = 10
    exgs = cfg_stk.exchg

    # generate new meta
    new_codes: List[str] = []
    for exg in exgs:
        for key in wt_asset[exg]:
            new_codes.append(key)
    dates = sorted([int(date) for date in wt_tradedays["CHINA"]])
    dates = [str(date) for date in dates]
    new_dates: Dict[str, List[str]] = {}
    cur_year = None
    # cur_month = None
    for date in dates:
        year = date[:4]
        month = date[4:6]
        day = date[6:8]
        if year != cur_year:
            new_dates[year] = []
            cur_year = year
        # if month != cur_month:
        #     new_dates[year][month] = []
        #     cur_month = month
        new_dates[year].append(month+day)

    new_metrics: List[str] = list(LXR_API.all_metrics)
    # ['pe_ttm', 'd_pe_ttm', 'pb', 'pb_wo_gw', 'ps_ttm', 'pcf_ttm', 'dyr', 'sp', 'spc', 'spa', 'tv', 'ta', 'to_r', 'shn', 'mc', 'mc_om', 'cmc', 'ecmc', 'ecmc_psh', 'fpa', 'fra', 'fb', 'ssa', 'sra', 'sb', 'ha_sh', 'ha_shm', 'mm_nba', 'ev_ebit_r', 'ev_ebitda_r', 'ey', 'pev']
    # commonly missing: ['fpa', 'fra', 'fb', 'ssa', 'sra', 'sb', 'ha_sh', 'ha_shm', 'mm_nba', 'ev_ebit_r', 'ev_ebitda_r', 'pev']

    # dates = pd.to_datetime(wt_tradedays["CHINA"], format='%Y%m%d').sort_values()
    # all_dates: List[datetime] = [
    #     date for date in dates if date < datetime.today()]

    meta = {
        'codes': new_codes,
        'dates': new_dates,
        'metrics': new_metrics,
    }

    meta_path = f"{path}/meta.json"
    if os.path.exists(meta_path):
        # check meta data
        # NOTE: metric, dates information will only be recorded as meta, check they are consistent
        old_meta = load_json(meta_path)
        old_codes: List[str] = old_meta['codes']
        old_dates: Dict[str, List[str]] = old_meta['dates']
        old_years = len(old_dates.keys())
        for idx, (year, month_and_days) in enumerate(old_dates.items()):
            # allow last year to have different dates (will be removed anyway)
            if idx != old_years-1:
                assert month_and_days == meta['dates'][year]
        old_metrics: List[str] = old_meta['metrics']
        assert old_metrics == meta['metrics']

    # numpy is also row major
    template = np.full((365, len(new_metrics)), np.nan, dtype=np.float32)

    # now we are sure that all the meta are the same, fell free to update data
    processed_assets = os.listdir(mkdir(f"{path}/"))

    for exg in exgs:
        pending_assets = []
        for key in wt_asset[exg]:
            if key not in processed_assets:
                pending_assets.append(key)
        if len(pending_assets) != 0:
            print(f"Updating {len(pending_assets)} fundamentals for: {exg}")
            for pending_asset in tqdm(pending_assets):
                fsTableType = wt_asset[exg][pending_asset]['extras']['fsTableType']
                ipoDate = wt_asset[exg][pending_asset]['extras']['ipoDate']
                ipoTime = datetime.fromisoformat(ipoDate)
                ranges = _generate_dt_ranges(
                    ipoTime.year, datetime.today().year, API_LIMITS)
                cur_year = None
                map = {}
                data = np.array([])
                for range in ranges:
                    items = LXR_API.query_fundamental(
                        fsTableType, range[0], range[1], [pending_asset])
                    for item in reversed(items):
                        date_str = item['date']
                        year = str(date_str[:4])
                        month = str(date_str[5:7])
                        day = str(date_str[8:10])
                        if year != cur_year:
                            # save yearly data as npy file
                            if data.size != 0:
                                store_compressed_array(
                                    data, f"{path}/{pending_asset}/{year}.npy")
                            # build new year map
                            map = {s: i for i, s in enumerate(new_dates[year])}
                            n = len(new_dates[year])
                            data = copy.deepcopy(template[:n, :])
                            cur_year = year
                        idx_d = map.get(month+day)
                        for idx_m, metric in enumerate(new_metrics):
                            data[idx_d, idx_m] = item.get(metric)

    return meta

# ================================================
# Others
# ================================================

def time_diff_in_min(start: int, end: int) -> int:
    from datetime import datetime

    def parse_time(time: int) -> datetime:
        time_str = str(time)
        # Extract time components from the last 10 characters of the string
        year = int(time_str[-12:-8])
        month = int(time_str[-8:-6])
        day = int(time_str[-6:-4])
        hour = int(time_str[-4:-2])
        minute = int(time_str[-2:])
        return datetime(year, month, day, hour, minute)
    # Parse both start and end strings into datetime objects
    start_time = parse_time(start)
    end_time = parse_time(end)
    # Calculate the difference in time
    delta = end_time - start_time
    # Convert the time difference to minutes and return it as an integer
    min_diff = int(delta.total_seconds() // 60)
    return min_diff

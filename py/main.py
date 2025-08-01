
import os
import sys
import subprocess
from typing import List, Dict

# the path include need to be earlier than relative library
TOP = "../"
sys.path.append(os.path.join(os.path.dirname(__file__), "."))
sys.path.append(os.path.join(os.path.dirname(__file__), TOP))
sys.path.append(os.path.join(os.path.dirname(__file__), "app"))
sys.path.append(os.path.join(os.path.dirname(__file__), "app/Exchange_API/"))


def run_cpp_build():
    '''Trigger the C++ build.bat file'''
    cpp_dir = os.path.join(os.path.dirname(__file__), TOP, "cpp")
    
    print("Triggering C++ build script...")
    result = subprocess.run(
        "build.bat", 
        cwd=cpp_dir, 
        shell=True
    )
    
    if result.returncode != 0:
        print(f"Build script exited with code: {result.returncode}")
        return False
    return True


def run_bt():
    from Util.UtilStk import prepare_all_files, mkdir
    from config.cfg_stk import cfg_stk
    wt_asset = prepare_all_files()

    # wt_assets: List[str] = []
    # ipo_dates: List[str] = []
    # for exg in cfg_stk.exchg:
    #     assets_exchg = os.listdir(mkdir(f"{cfg_stk.WT_STORAGE_DIR}/his/min1/{exg}/"))
    #     for key in wt_asset[exg]:
    #         if f"{key}.dsb" in assets_exchg:
    #             wt_assets.append(f'{exg}.{wt_asset[exg][key]['product']}.{key}')
    #             ipo_dates.append(wt_asset[exg][key]['extras']['ipoDate'])
    #
    # # cpu load balancing (from early to new)
    # parsed_ipo_dates = [datetime.fromisoformat(date[:-6]) for date in ipo_dates]
    # sorted_wt_assets = [x for _, x in sorted(zip(parsed_ipo_dates, wt_assets))][:cfg_stk.num]
    #
    # code_info: Dict[str, Dict] = {}
    # # prepare meta data
    # for idx, code in enumerate(sorted_wt_assets):
    #     code_info[code] = {'idx':idx}
    # print(wt_asset)

    # run_cpp_build()


if __name__ == '__main__':
    run_bt()

import pandas as pd
import openpyxl
from collections import Counter
from datetime import datetime

header = ["股票代碼", "股票名稱", "增加次數", "新增次數", "剔除次數"]


def read_and_clean_sheet(workbook, sheet_name, skip_rows, use_columns):
    data = pd.read_excel(
        workbook,
        sheet_name,
        skiprows=skip_rows,
        usecols=use_columns,
        header=None,
        converters={
            0: lambda x: str(int(x)) if not pd.isnull(x) and "." in str(x) else str(x)
        },
    )
    data_clean = data.dropna(how="all")  # 刪除所有值都是NaN的行

    data_clean.columns = [""] * data_clean.shape[1]
    return data_clean


def extract_stock_codes(df_list):
    """提取 DataFrame 列表中的股票代码并返回一个列表"""
    stock_codes = []
    stock_names = {}
    for df in df_list:
        for _, row in df.iterrows():
            code, name = row.iloc[0], row.iloc[1]
            stock_codes.append(code)
            stock_names[code] = name
    return stock_codes, stock_names


def write_excel(dataframe, file_path, sheet_name, header):
    with pd.ExcelWriter(
        file_path, engine="openpyxl", mode="a", if_sheet_exists="replace"
    ) as writer:
        dataframe.to_excel(writer, sheet_name=sheet_name, index=False, header=header)


def FundationCompare(file_path):
    # 打開Excel文件
    xls = pd.ExcelFile(file_path)
    # 獲取所有包含"基金"工作表名稱的資料
    fund_sheets = [sheet for sheet in xls.sheet_names if "基金" in sheet]
    # 創建一個空的DataFrame來儲存所有的數據
    increase_fund, new_fund, delete_fund = [], [], []
    # 遍歷所有含有"基金"的工作表，並將數據讀取到DataFrame中
    for sheet in fund_sheets:
        increase_fund.append(read_and_clean_sheet(xls, sheet, 16, "A:B"))
        new_fund.append(read_and_clean_sheet(xls, sheet, 16, "E:F"))
        delete_fund.append(read_and_clean_sheet(xls, sheet, 16, "I:J"))
    # 提取股票代码和名称
    increase_codes, increase_names = extract_stock_codes(increase_fund)
    new_codes, new_names = extract_stock_codes(new_fund)
    delete_codes, delete_names = extract_stock_codes(delete_fund)
    # 合并所有名称映射
    all_names = {**increase_names, **new_names, **delete_names}
    all_stock_codes = set(increase_codes + new_codes + delete_codes)

    rows = []  # 创建一个空列表来存储每行的数据
    # 对 all_stock_codes 中的每个股票代码，统计其在三个列表中的出现次数
    for stock_code in all_stock_codes:
        increase_count = Counter(increase_codes)[stock_code]
        new_count = Counter(new_codes)[stock_code]
        delete_count = Counter(delete_codes)[stock_code]
        rows.append(
            [
                stock_code,
                all_names.get(stock_code, "未知名称"),
                increase_count,
                new_count,
                delete_count,
            ]
        )

    # 创建一个DataFrame来存储结果
    results_df = pd.DataFrame(rows, columns=header)
    # 將增加次數與新增次數做統合，再根據這個統合值進行排序
    results_df["增加和新增總和"] = results_df["增加次數"] + results_df["新增次數"]
    results_df.sort_values(
        by=["增加和新增總和", "增加次數", "新增次數"],
        ascending=[False, False, False],
        inplace=True,
    )
    # 移除“增加和新增總和”這個臨時列，如果您不希望它出現在最終的Excel文件中
    results_df.drop("增加和新增總和", axis=1, inplace=True)

    print(results_df)
    write_excel(results_df, file_path, "Summary", header)


if __name__ == "__main__":

    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Construct the filename with the current date appended
    filename = f"FundRanking_{current_date}.xlsx"

    FundationCompare(filename)

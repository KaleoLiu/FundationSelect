import pandas as pd
from datetime import datetime
import concurrent.futures


def read_excel_sheet(file_path, sheet_name):
    # 讀取單一工作表數據
    xls = pd.ExcelFile(file_path)
    df = pd.read_excel(xls, sheet_name=sheet_name)
    return df


def extract_data(df, rows, cols):
    # 從數據框中提取數據
    data = df.iloc[rows, cols]
    return data


def FoundSelect(number=3, file_path=None):
    fund_types = [
        "國內股票開放型科技類",
        "國內股票開放型一般股票型",
        "國內股票開放型中小型",
    ]

    # 如果沒有提供文件路徑，使用當前日期構建文件名
    if file_path is None:
        # Get the current date in the format YYYY-MM-DD
        current_date = datetime.now().strftime("%Y-%m-%d")
        # Construct the filename with the current date appended
        file_path = f"FundRanking_{current_date}.xlsx"
    cols_to_extract = slice(0, 2)
    rows_to_extract = slice(0, number)

    # 只讀取「存在」的工作表（富邦 503 時可能缺某幾類）
    data_frames = []
    for sheet_name in fund_types:
        try:
            df = read_excel_sheet(file_path, sheet_name)
            data_frames.append(df)
        except ValueError:
            # Worksheet named '...' not found，略過該類
            continue

    if not data_frames:
        raise FileNotFoundError(
            f"Excel 中找不到任何排名工作表（{fund_types}），請先成功執行 FundationTaiwan 建檔"
        )

    # 提取數據
    extracted_data = [
        extract_data(df, rows_to_extract, cols_to_extract) for df in data_frames
    ]
    combined_data = pd.concat(extracted_data, ignore_index=True)
    return combined_data


if __name__ == "__main__":
    FoundSelect(3)

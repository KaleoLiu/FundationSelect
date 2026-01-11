import requests
import openpyxl
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
import threading
from datetime import datetime
import os
import json
import traceback

try:
    import certifi
except ImportError:
    certifi = None


def get_data(url, fund_type):
    # 配置Selenium
    # geckodriver_path = "/geckodriver"
    geckodriver_path = "/opt/homebrew/bin/geckodriver"

    options = Options()
    options.add_argument("--disable-notifications")
    options.add_argument("--headless")
    # 使用 Firefox 瀏覽器與指定的選項
    service = Service(executable_path=geckodriver_path)
    # service = Service(GeckoDriverManager().install())
    browser = webdriver.Firefox(service=service, options=options)
    browser.get(url)

    # 選擇下拉列表中的基金類型
    select_element = Select(browser.find_element("name", "selTID"))
    select_element.select_by_visible_text(fund_type)

    fund_type_url = browser.current_url

    # #region agent log
    log_path = "/Users/server-macmini/Documents/FundationSelect/.cursor/debug.log"
    cert_bundle = None
    try:
        cert_bundle = certifi.where() if certifi else None
        with open(log_path, "a") as f:
            f.write(
                json.dumps(
                    {
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "A",
                        "location": "FundationTaiwan.py:41",
                        "message": "certifi check",
                        "data": {
                            "certifi_available": certifi is not None,
                            "cert_bundle": cert_bundle,
                            "url": fund_type_url,
                        },
                        "timestamp": int(datetime.now().timestamp() * 1000),
                    }
                )
                + "\n"
            )
    except Exception as e:
        pass
    # #endregion

    # 使用 requests 库獲取頁面內容
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }
    session = requests.Session()

    # #region agent log
    try:
        with open(log_path, "a") as f:
            f.write(
                json.dumps(
                    {
                        "sessionId": "debug-session",
                        "runId": "run1",
                        "hypothesisId": "A,B,C,D,E",
                        "location": "FundationTaiwan.py:47",
                        "message": "before requests.get",
                        "data": {
                            "url": fund_type_url,
                            "verify_default": session.verify,
                        },
                        "timestamp": int(datetime.now().timestamp() * 1000),
                    }
                )
                + "\n"
            )
    except Exception as e:
        pass
    # #endregion

    try:
        response = session.get(fund_type_url, headers=headers)
        # #region agent log
        try:
            with open(log_path, "a") as f:
                f.write(
                    json.dumps(
                        {
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "A,B,C,D,E",
                            "location": "FundationTaiwan.py:54",
                            "message": "requests.get success",
                            "data": {"status_code": response.status_code},
                            "timestamp": int(datetime.now().timestamp() * 1000),
                        }
                    )
                    + "\n"
                )
        except Exception as e:
            pass
        # #endregion
    except requests.exceptions.SSLError as e:
        # #region agent log
        try:
            with open(log_path, "a") as f:
                f.write(
                    json.dumps(
                        {
                            "sessionId": "debug-session",
                            "runId": "run1",
                            "hypothesisId": "A,B,C,D,E",
                            "location": "FundationTaiwan.py:61",
                            "message": "SSL error caught",
                            "data": {
                                "error_type": type(e).__name__,
                                "error_msg": str(e),
                                "cert_bundle": cert_bundle if certifi else None,
                            },
                            "timestamp": int(datetime.now().timestamp() * 1000),
                        }
                    )
                    + "\n"
                )
        except Exception as log_err:
            pass
        # #endregion
        raise

    # 關閉瀏覽器
    browser.quit()

    return response.text


def parse_table(html):
    # 使用 BeautifulSoup 解析網頁內容
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    rows = table.find_all("tr")

    # data = pd.DataFrame()

    all_row_data = []  # 初始化用于存储所有行数据的列表

    # 解析表格的每一行，將資料保存到 DataFrame
    for row in rows:
        cols = row.find_all("td")
        if cols:
            row_data = [col.text.strip() for col in cols]
            # row_df = pd.DataFrame([row_data])
            # data = pd.concat([data, row_df], ignore_index=True)
            all_row_data.append(row_data)

    # 直接从 all_row_data 列表创建 DataFrame
    data = pd.DataFrame(all_row_data)

    # 定位表格中“上述資料”的行
    start_row = data[data.iloc[:, 0].str.contains("上述資料")].index[0]
    # 保留表格的前幾行，並且只保留前10列
    data = data.iloc[:start_row]
    data = data.iloc[2:, :10]

    return data


def create_empty_excel(file_name):
    # 創建一個空的 Excel 文件
    workbook = openpyxl.Workbook()
    workbook.save(file_name)


write_lock = threading.Lock()


def write_to_excel(data, sheet_name, File_name):
    # 获取锁
    with write_lock:
        workbook = openpyxl.load_workbook(File_name)
        if sheet_name in workbook.sheetnames:
            del workbook[sheet_name]
        worksheet = workbook.create_sheet(title=sheet_name)

        for row in data.values:
            worksheet.append(row.tolist())

        workbook.save(File_name)


# FundationGet函數，這裡我們將其設計為一個可以接受參數的函數，方便多線程處理
def FundationGet(fund_type, File_name):
    url = "https://fubon-ebrokerdj.fbs.com.tw/w/wq/wq02.djhtm"

    # 獲取網頁內容
    html = get_data(url, fund_type)
    # 解析表格並獲取數據
    data = parse_table(html)
    # 將數據寫入Excel
    write_to_excel(data, fund_type, File_name)


def FundationTaiwan(filename):

    # 定義需要處理的基金類型
    fund_types = [
        "國內股票開放型科技類",
        "國內股票開放型一般股票型",
        "國內股票開放型中小型",
    ]

    # If a file with the same name already exists, remove it
    if os.path.exists(filename):
        os.remove(filename)

    # Create an empty Excel file with the new filename
    create_empty_excel(filename)

    """# 如果已存在該Excel，則先移除
    if os.path.exists("FundRanking.xlsx"):
        os.remove("FundRanking.xlsx")
    # 創建一個空的Excel文件
    create_empty_excel("FundRanking.xlsx")
    """
    # 定義一個儲存多線程的list
    threads = []

    # 為每種基金類型開啟一個新的線程
    for fund_type in fund_types:
        # 創建新線程，目標函數為FundationGet，參數為fund_type
        thread = threading.Thread(target=FundationGet, args=(fund_type, filename))
        # 將新創建的線程添加到線程列表中
        threads.append(thread)
        # 開始執行新創建的線程
        thread.start()

    # 等待所有線程結束
    for thread in threads:
        thread.join()


if __name__ == "__main__":

    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Construct the filename with the current date appended
    filename = f"FundRanking_{current_date}.xlsx"

    FundationTaiwan(filename)

import os
from datetime import datetime
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from FoundationSelect import FoundSelect
import openpyxl
from threading import Thread
from datetime import date, timedelta
import calendar
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed


# 獲取基金持股連結
def get_fund_index_link(url):
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

    try:
        # 等待“月前十大”連結出現
        wait = WebDriverWait(browser, 10)
        link = wait.until(
            EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "月前十大"))
        )

        # 點擊該連結
        link.click()

        # 等待新頁面加載
        wait.until(EC.number_of_windows_to_be(2))
        # 切換到新打開的窗口
        browser.switch_to.window(browser.window_handles[-1])

        # 等待新頁面完全加載
        time.sleep(3)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # 返回新頁面的URL
        return browser.current_url
    except (TimeoutException, NoSuchElementException) as e:
        print("Error occurred: ", e)
        return None
    finally:
        # 關閉瀏覽器
        browser.quit()


# 最近一個月份：數據寫入Excel
def write_recrrent_month_excel(data, sheet_name, header, filename):
    workbook = openpyxl.load_workbook(filename)
    if sheet_name in workbook.sheetnames:
        del workbook[sheet_name]
    worksheet = workbook.create_sheet(title=sheet_name)

    # Insert the description at the top of the first row
    description = ["最近一個月資料"]
    worksheet.append(description)

    # 首先添加標頭
    worksheet.append(header)
    for row in data:
        worksheet.append(row)
    workbook.save(filename)


# 最近兩個月份：數據寫入Excel
def write_previous_month_excel(data, sheet_name, header, filename):
    workbook = openpyxl.load_workbook(filename)
    worksheet = workbook[sheet_name]

    # Add description to the first cell in the first row
    worksheet.cell(row=1, column=10, value="最近兩個月資料")

    # 添加標頭至第十行開頭
    for col_idx, header_title in enumerate(header, start=10):
        worksheet.cell(row=2, column=col_idx, value=header_title)

    # 從第一行第10列開始寫入多行數據
    for row_idx, row_data in enumerate(data, start=3):
        for col_idx, value in enumerate(row_data, start=10):
            worksheet.cell(row=row_idx, column=col_idx, value=value)

    workbook.save(filename)


# 兩個月份比較：數據寫入Excel
def write_compare_fundation_excel(increase, new, delete, sheet_name, filename):
    workbook = openpyxl.load_workbook(filename)
    worksheet = workbook[sheet_name]

    # 從第15行第1列開始寫入多行數據
    for row_idx, row_data in enumerate(increase, start=15):
        for col_idx, value in enumerate(row_data, start=1):
            worksheet.cell(row=row_idx, column=col_idx, value=value)

    # 從第15行第5列開始寫入多行數據
    for row_idx, row_data in enumerate(new, start=15):
        for col_idx, value in enumerate(row_data, start=5):
            worksheet.cell(row=row_idx, column=col_idx, value=value)

    # 從第15行第10列開始寫入多行數據
    for row_idx, row_data in enumerate(delete, start=15):
        for col_idx, value in enumerate(row_data, start=9):
            worksheet.cell(row=row_idx, column=col_idx, value=value)
    workbook.save(filename)


# 選擇部分文字的選項
def select_by_partial_text(select_element, partial_text):
    found = False
    for option in select_element.options:
        if partial_text in option.text:
            select_element.select_by_visible_text(option.text)
            found = True
            return
    if not found:
        raise NoSuchElementException(
            f"No option with partial text '{partial_text}' found in dropdown."
        )


# 主要函數
def search_each_fundation(month, Fundation_name, Fundation_company, fund_index_link):
    print("Fundation_name:", Fundation_name)
    print("Fundation_company:", Fundation_company)
    # 配置Selenium
    # geckodriver_path = "/geckodriver"
    geckodriver_path = "/opt/homebrew/bin/geckodriver"

    options = Options()
    options.add_argument("--disable-notifications")
    options.add_argument("--headless")
    # 使用 Firefox 瀏覽器與指定的選項
    service = Service(executable_path=geckodriver_path)
    browser = webdriver.Firefox(service=service, options=options)
    browser.get(fund_index_link)

    wait = WebDriverWait(browser, 10)

    try:
        # 選擇月份
        print("1:月份")
        wait = WebDriverWait(browser, 10)
        dropdown_element_time = wait.until(
            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_ddlQ_YM"))
        )
        time.sleep(3)
        dropdown = Select(dropdown_element_time)
        select_by_partial_text(dropdown, month)

        # 選擇基金
        print("2：基金")
        wait = WebDriverWait(browser, 10)
        dropdown_element = wait.until(
            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_ddlQ_Comid"))
        )
        time.sleep(3)
        dropdown = Select(dropdown_element)
        select_by_partial_text(dropdown, Fundation_company)

        # 點擊搜尋按鈕
        print("3：按鈕")
        wait = WebDriverWait(browser, 20)
        button = wait.until(
            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_BtnQuery"))
        )
        button.click()
        time.sleep(3)

        # 定位表格並提取目標行
        print("4提取基金名")
        grab_rows, grabbed_rows = False, 0
        search_text = Fundation_name.split("基金")[0] + "基金"
        print(search_text)

        # 更改這裡，一次性選取所有需要的<td>元素
        table_cells = browser.find_elements("xpath", "//tr")
        table_data = []

        for row in table_cells:
            cells = row.find_elements("xpath", ".//td")
            row_data = [cell.text for cell in cells]

            if search_text in row_data[0][0:20]:
                print("Sucess!!!!!")
                grab_rows = True

            if grab_rows and grabbed_rows < 10:
                if grabbed_rows == 0:
                    del row_data[0]  # 刪除第一行第一列的元素
                table_data.append(row_data)
                grabbed_rows += 1
                if grabbed_rows >= 10:
                    break

        # 將表格數據作為返回值
        return table_data
    except NoSuchElementException as e:
        print("Error creating")
    finally:
        browser.quit()
        print("END PUBLIC")


def calculate_business_days_in_month(year, month):
    """計算一個月中的工作日數量"""
    first_day = date(year, month, 1)
    last_day = (
        date(year, month + 1, 1) - timedelta(days=1)
        if month != 12
        else date(year, 12, 31)
    )

    # 生成該月的日期範圍
    all_days = [date(year, month, day) for day in range(1, last_day.day + 1)]

    # 計算工作日（排除週六和週日）
    weekdays = [d.weekday() for d in all_days]
    business_days = [d for d, wd in zip(all_days, weekdays) if wd < 5]

    return business_days


def get_current_and_previous_months(today):
    """根據今天是當月第幾個工作日來獲取當前月份和前一個月份"""
    current_month_business_days = calculate_business_days_in_month(
        today.year, today.month
    )
    # 檢查今天是否為工作日，如果不是則找到最接近的前一個工作日
    if today not in current_month_business_days:
        # 獲取比今天早的工作日列表
        earlier_business_days = [d for d in current_month_business_days if d < today]
        # 選擇最接近今天的工作日
        closest_business_day = (
            max(earlier_business_days) if earlier_business_days else None
        )
        if closest_business_day:
            todays_index_in_business_days = (
                current_month_business_days.index(closest_business_day) + 1
            )
        else:
            raise ValueError("No earlier business days found in the current month.")
    else:
        todays_index_in_business_days = current_month_business_days.index(today) + 1
    # 計算當前月份的第一天
    first_day_of_current_month = date(today.year, today.month, 1)

    if todays_index_in_business_days > 9:
        # 計算前一個月的最後一天
        current_month = first_day_of_current_month + timedelta(days=1)
    else:
        # 如果今天不是第十個工作日之後
        current_month = first_day_of_current_month - timedelta(days=1)

    days = current_month
    # 前一個月：特別處理當前月份為1月的情況
    if days.month == 1:
        previous_1month = date(days.year - 1, 12, 1)
    else:
        previous_1month = date(days.year, days.month - 1, 1)

    days = previous_1month
    # 前兩個月：特別處理當前月份為1月的情況
    if days.month == 1:
        previous_2month = date(days.year - 1, 12, 1)
    else:
        previous_2month = date(days.year, days.month - 1, 1)

    # 格式化為“年 年 月 月”格式
    formatted_previous_1month = previous_1month.strftime("%Y 年 %m 月")
    formatted_previous_2month = previous_2month.strftime("%Y 年 %m 月")

    return formatted_previous_1month, formatted_previous_2month


def compare_fundation_in_different_months(recent_months, previous_months):
    re_month_arrey = [[row[2], row[3], row[4]] for row in recent_months]
    pr_month_arrey = [[row[2], row[3], row[4]] for row in previous_months]

    # 初始化一個新的陣列來存儲匹配的項目
    increase_fundations = []
    increase_fundations.append("增加持股")
    pr = [
        "標的代號",
        "標的名稱",
        "金額",
        "差額",
    ]
    increase_fundations.append(pr)
    for pr in pr_month_arrey:
        for re in re_month_arrey:
            if pr[0] == re[0] and pr[2] <= re[2]:
                # 将字符串转换为浮点数后进行差值计算
                pr_diff = float(re[2].replace(",", "")) - float(pr[2].replace(",", ""))
                pr.append(pr_diff)  # 将差值追加到pr列表中
                increase_fundations.append(pr)

    # 初始化一個新的陣列來存儲匹配的項目
    new_fundations = []
    new_fundations.append("新增持股")
    pr = [
        "標的代號",
        "標的名稱",
        "金額",
    ]
    new_fundations.append(pr)
    for re in re_month_arrey:
        found = False  # 設置一個標誌來追踪是否找到匹配項目
        for pr in pr_month_arrey:
            if re[0] == pr[0]:
                found = True  # 找到匹配項目時，將標誌設為True
                break  # 如果找到匹配項目，則中斷內層迴圈
        if not found:  # 如果未找到匹配項目，且滿足其他條件
            new_fundations.append(re)  # 則添加到新陣列中

    # 初始化一個新的陣列來存儲匹配的項目
    delete_fundations = []
    delete_fundations.append("剔除持股")
    pr = [
        "標的代號",
        "標的名稱",
        "金額",
    ]
    delete_fundations.append(pr)
    for pr in pr_month_arrey:
        found = False  # 設置一個標誌來追踪是否找到匹配項目
        for re in re_month_arrey:
            if pr[0] == re[0]:
                found = True  # 找到匹配項目時，將標誌設為True
                break  # 如果找到匹配項目，則中斷內層迴圈
        if not found:  # 如果未找到匹配項目，且滿足其他條件
            delete_fundations.append(pr)  # 則添加到新陣列中

    # 回覆“增持股票”.“新增股票”.“剔除股票”
    return increase_fundations, new_fundations, delete_fundations


def process_fundation(Fundation, months, fund_monthly_link, filename, delay):
    results = []
    threads = []
    time.sleep(delay)  # 在任务执行前增加延迟
    # 使用多執行緒運行main函數並收集返回值
    for month in months:
        thread = Thread(
            target=lambda q, arg1, arg2, arg3, arg4: q.append(
                search_each_fundation(arg1, arg2, arg3, arg4)
            ),
            args=(
                results,
                month,
                Fundation[1]["基金名稱"],
                Fundation[1]["基金公司"],
                fund＿monthly_link,
            ),
        )
        threads.append(thread)
        thread.start()
        time.sleep(5)
    for thread in threads:
        thread.join()

    # 標頭資訊
    headers = [
        "名次",
        "標的種類",
        "標的代號",
        "標的名稱",
        "金額",
        "擔保機構",
        "次順位債券",
        "受益權單位數",
        "基金淨資產價值之比例",
    ]
    # 先寫入最新月份資料再寫入前一月份資料
    write_recrrent_month_excel(results[0], Fundation[1]["基金名稱"], headers, filename)
    write_previous_month_excel(results[1], Fundation[1]["基金名稱"], headers, filename)

    increase = []
    new = []
    delete = []
    increase, new, delete = compare_fundation_in_different_months(
        results[0], results[1]
    )
    write_compare_fundation_excel(
        increase, new, delete, Fundation[1]["基金名稱"], filename
    )
    print("/////////////")


def FundationHolding(filename):

    today = date.today()
    print("今天日期:", today)
    # 獲取當前月份和前一個月份
    formatted_current_month, formatted_previous_month = get_current_and_previous_months(
        today
    )
    print("當前月份:", formatted_current_month)
    print("前一個月份:", formatted_previous_month)

    # 抓取基金持股連結
    url = "https://www.sitca.org.tw/ROC/Industry/IN2002.aspx?PGMID=IN0202"
    fund＿monthly_link = get_fund_index_link(url)
    print(fund＿monthly_link)

    # 獲取基金選擇
    Fundations = FoundSelect(5)
    print(Fundations["基金名稱"])
    months = [formatted_current_month, formatted_previous_month]
    # 確保 threads 和 results 列表被正確初始化
    threads = []
    max_workers = 3  # 同時允許的最大執行緒數量
    delay = 2  # 初始延迟
    delay_increment = 7  # 每个任务之间增加的延迟
    if False:
        for Fundation in Fundations.iterrows():
            results = []
            # 使用多執行緒運行main函數並收集返回值
            for month in months:
                thread = Thread(
                    target=lambda q, arg1, arg2, arg3, arg4: q.append(
                        search_each_fundation(arg1, arg2, arg3, arg4)
                    ),
                    args=(
                        results,
                        month,
                        Fundation[1]["基金名稱"],
                        Fundation[1]["基金公司"],
                        fund＿monthly_link,
                    ),
                )
                threads.append(thread)
                thread.start()
            for thread in threads:
                thread.join()

            # 標頭資訊
            headers = [
                "名次",
                "標的種類",
                "標的代號",
                "標的名稱",
                "金額",
                "擔保機構",
                "次順位債券",
                "受益權單位數",
                "基金淨資產價值之比例",
            ]
            # 先寫入最新月份資料再寫入前一月份資料
            write_recrrent_month_excel(
                results[0], Fundation[1]["基金名稱"], headers, filename
            )
            write_previous_month_excel(
                results[1], Fundation[1]["基金名稱"], headers, filename
            )

            increase = []
            new = []
            delete = []
            increase, new, delete = compare_fundation_in_different_months(
                results[0], results[1]
            )
            write_compare_fundation_excel(
                increase, new, delete, Fundation[1]["基金名稱"], filename
            )
            print("/////////////")
    else:
        try:
            # 使用ThreadPoolExecutor创建线程池
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for index, Fundation in enumerate(Fundations.iterrows()):
                    future = executor.submit(
                        process_fundation,
                        Fundation,
                        months,
                        fund_monthly_link,
                        filename,
                        delay + index * delay_increment,
                    )
                    futures.append(future)

                # 等待所有任务完成，可以在这里获取每个任务的结果
                for future in futures:
                    try:
                        result = future.result()  # 获取结果
                        # 处理结果
                    except Exception as e:
                        print(f"处理过程中发生错误: {e}")
                        print(f"Error result: {result}")

        except KeyboardInterrupt:
            print("程序被用户中断")
            # 在这里添加任何清理或保存工作的代码


if __name__ == "__main__":

    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now().strftime("%Y-%m-%d")
    # Construct the filename with the current date appended
    filename = f"FundRanking_{current_date}.xlsx"

    FundationHolding(filename)

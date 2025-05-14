import re
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup


class StockListingChecker:
    def jpx_new_listings(self) -> pd.DataFrame:
        """上場銘柄データを取得
        
        「新規上場会社情報」ページから 上場日／会社名／証券コード／市場区分 を
        スクレイピングし、`pandas.DataFrame` に整形して返します。
        """
        URL = "https://www.jpx.co.jp/listing/stocks/new/index.html"

        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        # エンコーディングを設定
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, "html.parser")

        # 必要なテーブル情報を抽出
        table = soup.find("table")  # ページ内の最初のテーブルを取得
        rows = table.find_all("tr")[1:]  # ヘッダーをスキップ
        even_rows = rows[1::2]
        odd_rows = rows[2::2]
        
        listing_df = pd.DataFrame(columns=["更新日", "銘柄名", "コード"])
        sub_df = pd.DataFrame(columns=["市場区分"])
        for row in even_rows:
            cols = [re.sub(r'\s+', ' ', col.text).strip() for col in row.find_all("td")]
            day = cols[0].split(' ')[0].strip()
            new_row = pd.DataFrame([{"更新日": datetime.strptime(day, "%Y/%m/%d"),
                                        "銘柄名": cols[1].replace("（株）", "").replace("代表者インタビュー", "").strip(),
                                        "コード": cols[2].strip()}])
            listing_df = pd.concat([listing_df, new_row], ignore_index=True)

        for row in odd_rows:
            cols = [re.sub(r'\s+', ' ', col.text).strip() for col in row.find_all("td")]    
            new_row = pd.DataFrame([{"市場区分" : cols[0]}])
            sub_df = pd.concat([sub_df, new_row], ignore_index=True)
        
        listing_df = pd.concat([listing_df, sub_df],axis=1)
        return listing_df

    def jpx_delisted(self) -> pd.DataFrame:
        """廃止銘柄データを取得"""
        URL = "https://www.jpx.co.jp/listing/stocks/delisted/index.html"

        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 必要なテーブル情報を抽出
        table = soup.find("table")  # ページ内の最初のテーブルを取得
        rows = table.find_all("tr")[1:]  # ヘッダーをスキップ

        delisted_df = pd.DataFrame(columns=["更新日", "銘柄名", "コード", "市場区分", "上場廃止理由"])
        for row in rows:
            cols = [re.sub(r'\s+', ' ', col.text).strip() for col in row.find_all("td")]
            day = cols[0].strip()
            new_row = pd.DataFrame([{"更新日": datetime.strptime(day, "%Y/%m/%d"),
                                        "銘柄名": cols[1].replace("（株）", "").strip(),
                                        "コード": cols[2].strip(),
                                        "市場区分": cols[3].strip(),
                                        "上場廃止理由": cols[4].strip()
                                        }])
            delisted_df = pd.concat([delisted_df, new_row], ignore_index=True)
        return delisted_df

if __name__ == "__main__":
    slc = StockListingChecker()
    cleaned = slc.jpx_new_listings()
    print(cleaned)
import re
import time
from typing import List
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

import os
import datetime

URL = "https://ca.image.jp/matsui/"

class SplitAdjustment:
    """
    更新日 / コード / 銘柄名 / 市場区分 / 分割比率
    を取得して既存の株価データを更新します。
    """
    def __init__(self):
        self.URL = URL

    def fetch_split_html(self,type: int, page: int = 1, seldate: int = 3) -> str:
        """
        指定ページの HTML を取得して文字列で返す
        type : 0=株式分割, 5=株式合併
        seldate : 0=すべて, 1=今日, 2=1週間, 3=1ヶ月, 4=3ヶ月, 5=日付指定
        """
        params = {
            "type": type,
            "sort": 1,                 # 日付昇順
            "seldate": seldate,        # 期間フィルタ
            "page": page,
            "word1": "",
            "word2": "",
            "serviceDatefrom": "",
            "serviceDateto": "",
        }
        resp = requests.get(self.URL, params=params, timeout=10)
        resp.raise_for_status()
        # 文字コード自動判定（Shift_JIS になる場合がある）
        resp.encoding = resp.apparent_encoding
        return resp.text


    def parse_split_html(self, html: str) -> pd.DataFrame:
        """
        取得した HTML から表を抽出して DataFrame で返す
        - まず pandas.read_html で <table> を自動抽出
        - 取れない場合は BeautifulSoup で手動パース
        """
        # 手動で <tr><td> をたどる
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for tr in soup.select("table tr")[1:]:  # 0 行目はヘッダー
            tds = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(tds) < 7:
                # 想定より短ければスキップ
                continue

            # 0:日付,1:コード,2:銘柄名,3:市場,
            # 4,5,6 がそれぞれ "1", ":", "2" など
            ratio = "".join(tds[4:7]).replace(" ", "").replace("\u3000", "")
            rows.append([tds[0], tds[1], tds[2], tds[3], ratio])
            # cols = [
            #     td.get_text(strip=True).replace("\u3000", " ").replace("\xa0", " ")
            #     for td in tr.find_all(["td", "th"])
            # ]
            # if len(cols) >= 5:
            #     rows.append(cols[:5])
        return pd.DataFrame(
            rows,
            columns=["更新日", "コード", "銘柄名", "市場区分", "分割比率"],
        )

    def ratio_to_float(self, ratio: str) -> float:
        """
        '1:2', '1 : 2', '１：２' などを 0.5 に変換
        - 左辺 ÷ 右辺 を返す（片方でも欠ければ NaN）
        """
        if not isinstance(ratio, str):
            return np.nan

        # 全角 → 半角、空白除去
        pre_ratio = (
            ratio.replace("：", ":")
            .replace(" ", "")
            .replace("\u3000", "") # 全角スペース削除
        )

        m = re.match(r"^(\d+(?:\.\d+)?)[:](\d+(?:\.\d+)?)$", pre_ratio)
        if not m:
            return np.nan

        left, right = map(float, m.groups())
        if right == 0:
            return np.nan

        return left / right

    def scrape_all(self,type: int, seldate: int = 3, max_pages: int = 3, delay: float = 1.5) -> pd.DataFrame:
        """
        ページ送りしながら最大 max_pages ページ分を結合して返す
        - 途中でデータが取れなくなったら終了
        - 各ページ 1.5 秒スリープ
        """
        all_frames = []
        for page in range(1, max_pages + 1):
            html = self.fetch_split_html(type, page, seldate)
            df = self.parse_split_html(html)

            if df.empty:
                break

            all_frames.append(df)

            # 「次のページが無い」判定：行数が 20 行未満なら最後とみなす
            if len(df) < 20:
                break

            time.sleep(delay)

        if not all_frames:
            return pd.DataFrame(
                columns=["更新日", "銘柄コード", "銘柄名", "市場", "分割比率"]
            )

        merged = pd.concat(all_frames, ignore_index=True)
        # 日付を datetime に
        merged["更新日"] = pd.to_datetime(merged["更新日"], errors="coerce")
        merged["分割比率"] = merged["分割比率"].apply(self.ratio_to_float)
        return merged

    # 既存の株価データと株式分割情報を利用して修正
    def apply_split_adjustments(self, stock_prices, split_data):
        today = datetime.datetime.now().date()
        pre_stock_prices = stock_prices.copy()
        
        # 当日の株価データは修正後の値であるため、当日以前のデータを修正
        if today in split_data['更新日'].values:       
            for _, row in split_data.iterrows():
                ticker = row["コード"] 
                ratio = row["分割比率"]
                # 該当銘柄の分割日前のデータを修正
                mask = (pre_stock_prices['コード'] == ticker) & (pre_stock_prices['日付'] < today)
                pre_stock_prices.loc[mask, ['Open', 'High', 'Low', 'Close']] *= ratio
                pre_stock_prices.loc[mask, 'volume'] /= ratio
        return pre_stock_prices

# if __name__ == "__main__":
#     sa = SplitAdjustment()
#     # 直近 1 ヶ月分を取得
#     type_0 = sa.scrape_all(type=0, seldate=3)
#     type_5 = sa.scrape_all(type=5, seldate=3)

#     print(type_0.head())
#     print("======================================")
#     print(type_5.head())
    
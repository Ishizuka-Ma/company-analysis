import re
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
import logging

# ロガーの設定
from src.utils.log_config import logger


class StockListingChecker:
    def __init__(self, processed_data_dir: str = "data/processed"):
        """初期化

        Args:
            processed_data_dir: 処理済みデータディレクトリのパス
        """
        self.processed_data_dir = Path(processed_data_dir)
        self.listing_data_dir = self.processed_data_dir / "listing"
        self.listing_data_dir.mkdir(parents=True, exist_ok=True)
        self.preprocess_data_path = self.listing_data_dir / "preprocess_data_j.csv"

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

    def load_existing_data(self) -> pd.DataFrame:
        """既存のpreprocess_data_j.csvを読み込む"""
        if not self.preprocess_data_path.exists():
            # ファイルが存在しない場合は空のDataFrameを返す
            return pd.DataFrame(columns=["更新日", "コード", "銘柄名", "市場区分", "33業種区分", "17業種区分"])
        
        try:
            return pd.read_csv(self.preprocess_data_path)
        except Exception as e:
            logger.debug(f"既存データの読み込みエラー: {e}")
            return pd.DataFrame(columns=["更新日", "コード", "銘柄名", "市場区分", "33業種区分", "17業種区分"])

    def save_updated_data(self, df: pd.DataFrame) -> None:
        """更新したデータをpreprocess_data_j.csvに保存する"""
        try:
            df.to_csv(self.preprocess_data_path, index=False)
            logger.debug(f"データを保存しました: {self.preprocess_data_path}")
        except Exception as e:
            logger.debug(f"データ保存エラー: {e}")

    def update_listing_data(self) -> None:
        """新規上場および廃止銘柄を反映してpreprocess_data_j.csvを更新する"""
        # 既存データの読み込み
        existing_data = self.load_existing_data()
        
        # 新規上場情報の取得
        new_listings = self.jpx_new_listings()
        # 廃止銘柄情報の取得
        delisted = self.jpx_delisted()
        
        # 本日分の新規上場と廃止銘柄を抽出
        today = datetime.datetime.now().date()
        today_new_listings = new_listings[new_listings["更新日"] == today]
        today_delisted = delisted[delisted["更新日"] == today]
        
        # 更新処理
        if not today_new_listings.empty:
            logger.debug(f"{len(today_new_listings)}件の新規上場銘柄があります")
            
            for _, row in today_new_listings.iterrows():
                # 既に同じコードが存在するか確認
                if row["コード"] in existing_data["コード"].values:
                    logger.debug(f"銘柄コード {row['コード']} は既に存在します")
                    continue
                
                # 新規データを追加
                new_row = pd.DataFrame([{
                    "更新日": row["更新日"],
                    "銘柄名": row["銘柄名"],
                    "コード": row["コード"],
                    "市場区分": row["市場区分"]
                }])
                existing_data = pd.concat([existing_data, new_row], ignore_index=True)
                logger.debug(f"銘柄 {row['銘柄名']}({row['コード']}) を追加しました")
        
        if not today_delisted.empty:
            logger.debug(f"{len(today_delisted)}件の上場廃止銘柄があります")
            
            for _, row in today_delisted.iterrows():
                # 指定したコードのデータを削除
                if row["コード"] in existing_data["コード"].values:
                    existing_data = existing_data[existing_data["コード"] != row["コード"]]
                    logger.debug(f"銘柄 {row['銘柄名']}({row['コード']}) を削除しました")
        
        # 更新したデータを保存
        self.save_updated_data(existing_data)
        logger.debug("上場銘柄データの更新が完了しました")

# if __name__ == "__main__":
#     slc = StockListingChecker()
#     slc.update_listing_data()
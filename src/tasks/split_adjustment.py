import re
import time
from typing import List, Dict, Union, Optional
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import datetime
from pathlib import Path
import logging

# ロガーの設定
from src.utils.log_config import logger

URL = "https://ca.image.jp/matsui/"

class SplitAdjustment:
    """
    更新日 / コード / 銘柄名 / 市場区分 / 分割比率
    を取得して既存の株価データを更新します。
    """
    def __init__(self, processed_data_dir: str = "data/processed"):
        """初期化
        
        Args:
            processed_data_dir: 処理済みデータディレクトリのパス
        """
        self.URL = URL
        self.processed_data_dir = Path(processed_data_dir)
        self.prices_dir = self.processed_data_dir / "prices"
        
        self.prices_dir.mkdir(parents=True, exist_ok=True)

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

    def load_stock_price(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        指定した銘柄の株価データを読み込む
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            株価データのDataFrame、存在しない場合はNone
        """
        file_path = self.prices_dir / f"{ticker}.parquet"
        
        if not file_path.exists():
            logger.warning(f"銘柄 {ticker} の株価データファイルが存在しません: {file_path}")
            return None
        
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            logger.error(f"株価データの読み込みエラー ({file_path}): {e}")
            return None
    
    def save_stock_price(self, ticker: str, df: pd.DataFrame) -> None:
        """
        株価データを保存する
        
        Args:
            ticker: 銘柄コード
            df: 保存する株価データ
        """
        if df is None or df.empty:
            logger.warning(f"銘柄 {ticker} の保存する株価データがありません")
            return
            
        file_path = self.prices_dir / f"{ticker}.parquet"
        
        try:
            df.to_parquet(file_path)
            logger.info(f"銘柄 {ticker} の株価データを保存しました: {file_path}")
        except Exception as e:
            logger.error(f"株価データの保存エラー ({file_path}): {e}")

    def apply_split_adjustment(self, ticker: str, ratio: float) -> bool:
        """
        指定した銘柄の株価データに分割比率を適用する
        
        Args:
            ticker: 銘柄コード
            ratio: 分割比率
            
        Returns:
            処理成功したらTrue、失敗したらFalse
        """
        df = self.load_stock_price(ticker)
        if df is None:
            return False
            
        try:
            # 当日以前のデータを修正（当日の株価データは修正後の値であるため）
            today = datetime.datetime.now().date()
            
            # 日付カラムがインデックスの場合とカラムの場合を考慮
            if df.index.name == "Date" or isinstance(df.index, pd.DatetimeIndex):
                mask = df.index.date < today
                # 株価と出来高を調整
                price_columns = ["Open", "High", "Low", "Close"]
                volume_column = "Volume"
                
                # 実際に存在するカラムだけを処理
                existing_price_cols = [col for col in price_columns if col in df.columns]
                
                if existing_price_cols:
                    df.loc[mask, existing_price_cols] *= ratio
                
                if volume_column in df.columns:
                    df.loc[mask, volume_column] /= ratio
            else:
                # 日付がカラムの場合
                date_col = next((col for col in df.columns if "date" in col.lower()), None)
                
                if date_col:
                    mask = pd.to_datetime(df[date_col]).dt.date < today
                    
                    # 株価カラムを特定
                    price_cols = [col for col in df.columns if any(name in col.lower() for name in ["open", "high", "low", "close", "price", "adj"])]
                    volume_col = next((col for col in df.columns if "volume" in col.lower()), None)
                    
                    if price_cols:
                        for col in price_cols:
                            df.loc[mask, col] *= ratio
                    
                    if volume_col:
                        df.loc[mask, volume_col] /= ratio
            
            # 更新したデータを保存
            self.save_stock_price(ticker, df)
            logger.info(f"銘柄 {ticker} の株価データを分割比率 {ratio} で調整しました")
            logger.info(f"銘柄 {ticker} \n df.tail()")
            return True
            
        except Exception as e:
            logger.error(f"分割調整処理エラー (銘柄 {ticker}): {e}")
            return False

    def update_split_adjustments(self) -> None:
        """
        本日の株式分割・株式合併情報を取得して株価データを調整する
        """
        # 株式分割情報取得
        split_df = self.scrape_all(type=0, seldate=1)  # type=0: 株式分割, seldate=1: 今日
        
        # 株式合併情報取得
        merger_df = self.scrape_all(type=5, seldate=1)  # type=5: 株式合併, seldate=1: 今日
        
        # 両方をマージ
        all_df = pd.concat([split_df, merger_df], ignore_index=True)
        
        if all_df.empty:
            logger.info("本日の株式分割・株式合併情報はありません")
            return
            
        # 本日の株式分割・株式合併を適用
        today = datetime.datetime.now().date()
        today_df = all_df[all_df["更新日"].dt.date == today]
        
        if today_df.empty:
            logger.info("本日の株式分割・株式合併情報はありません")
            return
            
        # 銘柄ごとに株価調整を実行
        success_count = 0
        for _, row in today_df.iterrows():
            ticker = row["コード"]
            ratio = row["分割比率"]
            
            if pd.isna(ratio):
                logger.warning(f"銘柄 {ticker} の分割比率が無効です")
                continue
                
            if self.apply_split_adjustment(ticker, ratio):
                success_count += 1
                
        logger.info(f"本日の株式分割・株式合併調整を {success_count}/{len(today_df)} 件適用しました")

# if __name__ == "__main__":
#     # ロガー設定
#     app_logger  # グローバルロガー設定を確実に読み込む
    
#     sa = SplitAdjustment()
#     sa.update_split_adjustments()
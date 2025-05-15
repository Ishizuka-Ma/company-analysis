"""
株価データ取得モジュール

このモジュールは、外部API（Yahoo Finance）から株価データを取得し、
ローカルにParquet形式で保存する機能を提供します。

主な機能:
- 株価の日次データ（OHLCV: Open, High, Low, Close, Volume）の取得
- 最新の取得日時管理によるデータの差分更新
- 取得データの保存とメタデータ管理
"""
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union
from enum import Enum

import pandas as pd
import yfinance as yf

# ロガーの設定
from src.utils.log_config import logger

class DefaultDate(Enum):
    OLDEST_AVAILABLE = "1970-01-01" # yfinanceでは1970年1月1日が最古のデータとして取得可能

class StockDataFetcher:
    """
    株価データ取得クラス
    
    Yahoo Financeから株価データを取得し、Parquetファイルに保存します。
    取得した最終日付をメタデータとして管理し、次回実行時には差分のみを取得する
    増分更新の仕組みを実装しています。
    
    Attributes:
        meta_file_path (Path): メタデータ保存先のファイルパス
        raw_data_path (Path): 生データの保存先ディレクトリ
        metadata (Dict): 最新の取得日などを保存するメタデータ
    """
    
    def __init__(self, meta_file_path: str, raw_data_path: str):
        """
        初期化
        
        Args:
            meta_file_path: メタデータファイルのパス
                            (例: "data/meta/latest_date.json")
            raw_data_path: 生データ保存先のディレクトリパス
                           (例: "data/raw")
        
        初期化処理の流れ:
        1. 引数で受け取ったパスをPathオブジェクトに変換
        2. 必要なディレクトリ構造を作成
        3. メタデータを読み込み
        """
        self.meta_file_path = Path(meta_file_path)
        self.raw_data_path = Path(raw_data_path)
        
        # メタデータディレクトリがなければ作成
        # parents=True: 親ディレクトリも含めて作成
        # exist_ok=True: すでに存在していてもエラーにしない
        self.meta_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 株価データは "prices" サブディレクトリに保存
        Path(self.raw_data_path / "prices").mkdir(parents=True, exist_ok=True)
        
        # メタデータ読み込み - 前回の実行情報を取得
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> Dict:
        """
        メタデータ読み込み用の内部メソッド
        
        Returns:
            Dict: メタデータを含む辞書オブジェクト
                 {
                    "last_updated": "YYYY-MM-DD", 
                    "tickers": {"7203.T": "YYYY-MM-DD", ...}
                 }
        
        メタデータファイルが存在しない場合や読み込みエラー時は、
        空のメタデータ辞書を返します。
        """
        if self.meta_file_path.exists():
            try:
                with open(self.meta_file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"メタデータ読み込みエラー: {e}")
                return {"last_updated": None, "tickers": {}}
        # ファイルが存在しない場合は空のメタデータを返す
        return {"last_updated": None, "tickers": {}}
    
    def _save_metadata(self):
        """
        メタデータ保存用の内部メソッド
        
        self.metadataの内容をJSONファイルとして保存します。
        """
        with open(self.meta_file_path, "w", encoding="utf-8") as f:
            # 辞書をJSON形式で保存
            # ensure_ascii=False: 日本語などの非ASCII文字をそのまま出力
            # indent=2: 読みやすいようにインデントを付ける
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
    
    def get_latest_date(self, ticker: str) -> Optional[str]:
        """
        指定した銘柄の最新の取得日を取得
        
        Args:
            ticker: 銘柄コード (例:"7203.T")
            
        Returns:
            最新取得日 (YYYY-MM-DD形式の文字列) または None (未取得の場合)
        """
        # 辞書のgetメソッドを使って値を取得（存在しない場合はNoneを返す）
        return self.metadata["tickers"].get(ticker)
    
    def fetch_stock_data(self, ticker: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        株価データをYahoo Financeから取得
        
        Args:
            ticker: 銘柄コード（日本株の場合は数字のみ or 数字.Tの形式）
                   例: "7203" (トヨタ自動車)、"9984.T" (ソフトバンクグループ)
            start_date: 開始日 (YYYY-MM-DD形式)。省略時は30年前から。
            end_date: 終了日 (YYYY-MM-DD形式)。省略時は今日まで。
            
        Returns:
            株価データのDataFrame
            
        取得される列:
        - Open: 始値
        - High: 高値
        - Low: 安値
        - Close: 終値
        - Adj Close: 調整後終値（株式分割などを調整済み）
        - Volume: 出来高
        """
        # 日本株の場合、ティッカーを調整（数字のみの場合は.Tを追加）
        if ticker.isdigit():
            ticker = f"{ticker}.T"
        
        # 日付指定がない場合は取得可能な最古のデータを指定する
        if not start_date:
            start_date = DefaultDate.OLDEST_AVAILABLE.value
        
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"株価データ取得開始: {ticker} ({start_date} から {end_date}まで)")
        
        try:
            # yfinanceライブラリを使用してデータ取得
            df = yf.download(ticker, start=start_date, end=end_date)
            
            if df.empty:
                logger.warning(f"データなし: {ticker}")
                return pd.DataFrame()
                
            # 取得結果のフォーマット調整
            df.index.name = "Date"  # インデックスの名前を設定
            df.columns = [col.title() for col in df.columns]  # 列名を先頭大文字に
            
            # 最新日付更新
            if not df.empty:
                latest_date = df.index[-1].strftime("%Y-%m-%d")
                # メタデータを更新
                self.metadata["tickers"][ticker] = latest_date
                self.metadata["last_updated"] = datetime.now().strftime("%Y-%m-%d")
                self._save_metadata()
            
            return df
            
        except Exception as e:
            # エラー発生時はログ出力して空のDataFrameを返す
            logger.error(f"データ取得エラー ({ticker}): {e}")
            return pd.DataFrame()
    
    def save_stock_data(self, ticker: str, data: pd.DataFrame, mode: str = "full"):
        """
        株価データを保存
        
        Args:
            ticker: 銘柄コード
            data: 株価データ
            mode: 保存モード ('full'または'incr')
        """
        if data.empty:
            logger.warning(f"保存する株価データなし: {ticker}")
            return
        
        # 日本株の場合、ティッカーからファイル名を取得
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        # ファイルパス設定
        save_dir = self.raw_data_path / "prices" / mode
        save_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = save_dir / f"{file_ticker}.parquet"
        
        # データ保存
        try:
            data.to_parquet(file_path)
            logger.info(f"株価データ保存完了: {file_path}")
        except Exception as e:
            logger.error(f"株価データ保存エラー ({file_path}): {e}")
    
    def fetch_and_save_stock_data(self, ticker: str, full_load: bool = False):
        """
        株価データを取得して保存
        
        Args:
            ticker: 銘柄コード
            full_load: 全期間取得フラグ
        """
        # 最新取得日確認
        latest_date = self.get_latest_date(ticker)
        
        # 取得期間設定
        if full_load or not latest_date:
            # 全期間取得
            df = self.fetch_stock_data(ticker)
            mode = "full"
        else:
            # 差分取得
            start_date = (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")
            
            if start_date >= end_date:
                logger.info(f"取得期間なし: {ticker} (最新: {latest_date})")
                return
                
            df = self.fetch_stock_data(ticker, start_date, end_date)
            mode = "incr"
        
        self.save_stock_data(ticker, df, mode)
    
    def fetch_company_list(self, market: str = "ALL") -> pd.DataFrame:
        """
        企業リストの取得（ダミー実装）
        実際には東証などからExcelファイルをダウンロードして処理
        
        Args:
            market: 市場区分 ("ALL", "プライム", "スタンダード", "グロース")
            
        Returns:
            企業リストのDataFrame
        """
        # ダミーデータ作成（実際のプロジェクトでは外部からダウンロード）
        dummy_data = {
            "コード": ["7203", "9984", "6758", "6861", "4755"],
            "銘柄名": ["トヨタ自動車", "ソフトバンクグループ", "ソニーグループ", "キーエンス", "楽天グループ"],
            "市場": ["プライム", "プライム", "プライム", "プライム", "プライム"]
        }
        
        df = pd.DataFrame(dummy_data)
        
        # 市場で絞り込み
        if market != "ALL":
            df = df[df["市場"] == market]
            
        return df 
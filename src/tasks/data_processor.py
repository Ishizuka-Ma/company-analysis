"""
株価データ処理モジュール

このモジュールは、取得した株価データの処理と特徴量生成をします。
生データを整形し、機械学習モデルで使用するための特徴量を作成します。

主な機能:
- 株価データの整形・クレンジング
- 欠損値の処理と異常値検出
- テクニカル指標の計算
- 機械学習用特徴量の生成
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import polars as pl

# ロガーの設定
from src.utils.log_config import logger

class StockDataProcessor:
    """
    株価データ処理クラス
    
    取得した生の株価データを処理し、バックテスト用の特徴量を生成します。
    処理済みデータと特徴量データをParquetファイルとして保存します。
    
    Attributes:
        raw_data_path (Path): 生データの読み込み元ディレクトリ
        processed_data_path (Path): 処理済みデータの保存先ディレクトリ
        feature_data_path (Path): 特徴量データの保存先ディレクトリ
    """
    
    def __init__(self, raw_data_path: str, processed_data_path: str, feature_data_path: str):
        """
        初期化
        
        Args:
            raw_data_path: 生データパス
            processed_data_path: 処理済みデータパス
            feature_data_path: 特徴量データパス
        """
        self.raw_data_path = Path(raw_data_path)
        self.processed_data_path = Path(processed_data_path)
        self.feature_data_path = Path(feature_data_path)
        
        # ディレクトリ作成
        self.processed_data_path.mkdir(parents=True, exist_ok=True)
        Path(self.processed_data_path / "prices").mkdir(parents=True, exist_ok=True)
        
        self.feature_data_path.mkdir(parents=True, exist_ok=True)
    
    def load_raw_stock_data(self, ticker: str, mode: str = "full") -> pd.DataFrame:
        """
        生の株価データ読み込み
        
        Args:
            ticker: 銘柄コード
            mode: 読み込みモード ('full' または 'incr')
                  - full: 全期間データ
                  - incr: 増分データ
            
        Returns:
            株価データのDataFrame
            
        生のデータファイルが存在しない場合や読み込みエラー時は、
        空のDataFrameを返します。
        """
        # ティッカーからファイル名を取得
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        # ファイルパス設定
        file_path = self.raw_data_path / "prices" / mode / f"{file_ticker}.parquet"
        
        if not file_path.exists():
            logger.warning(f"ファイルが存在しません: {file_path}")
            return pd.DataFrame()
        
        # データ読み込み
        try:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            logger.error(f"データ読み込みエラー ({file_path}): {e}")
            return pd.DataFrame()
    
    def process_stock_data(self, ticker: str, data: pd.DataFrame) -> pd.DataFrame:
        """
        株価データの処理
        
        Args:
            ticker: 銘柄コード
            data: 株価データ
            
        Returns:
            処理済み株価データ
            
        処理内容:
        1. データ型の調整
        2. 列名の標準化
        3. 欠損値の検出と補間
        4. 重複日付の削除
        5. 日付順のソート
        """
        if data.empty:
            return pd.DataFrame()
        
        # データ型の調整
        df = data.copy()
        
        # 列名の標準化
        standard_columns = [
            "Open", "High", "Low", "Close", "Adj Close", "Volume"
        ]
        
        # 存在する列だけマッピング
        existing_columns = []
        for col in standard_columns:
            title_col = col.title()
            if title_col in df.columns:
                existing_columns.append(title_col)
        
        df = df[existing_columns]
        
        # 欠損値チェック
        # df.isna()で各セルの欠損値をTrue/Falseで表現し、sum()で列ごとの欠損値数を集計、
        # さらにsum()でデータフレーム全体の欠損値の合計を取得
        missing_count = df.isna().sum().sum()
        if missing_count > 0:
            logger.warning(f"{ticker}: {missing_count}個の欠損値があります")
            
            # 前方補間で欠損値を埋める
            df = df.fillna(method="ffill")
            remaining_missing = df.isna().sum().sum()
            
            if remaining_missing > 0:
                # 後方補間で残りの欠損値を埋める
                df = df.fillna(method="bfill")
                
        # 日付インデックスの確認と調整
        if df.index.name != "Date":
            df.index.name = "Date"
            
        # 重複インデックスの削除
        if df.index.duplicated().any():
            logger.warning(f"{ticker}: 重複インデックスがあります")
            df = df[~df.index.duplicated(keep="last")]
            
        # 日付順にソート
        df = df.sort_index()
            
        return df
    
    def save_processed_data(self, ticker: str, data: pd.DataFrame):
        """
        処理済みデータの保存
        
        Args:
            ticker: 銘柄コード
            data: 処理済み株価データ
            
        処理済みのデータをParquetファイルとして保存します。
        ファイルはprocessed_data_path/prices/ディレクトリに保存されます。
        """
        if data.empty:
            logger.warning(f"保存する処理済みデータなし: {ticker}")
            return
        
        # ティッカーからファイル名を取得
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        # ファイルパス設定
        file_path = self.processed_data_path / "prices" / f"{file_ticker}.parquet"
        
        # データ保存
        try:
            data.to_parquet(file_path)
            logger.info(f"処理済みデータ保存完了: {file_path}")
        except Exception as e:
            logger.error(f"処理済みデータ保存エラー ({file_path}): {e}")
    
    def merge_stock_data(self, ticker: str, mode: str = "full"):
        """
        株価データのマージ処理
        
        Args:
            ticker: 銘柄コード
            mode: 処理モード ('full' または 'incr')
                  - full: 全量データを上書き
                  - incr: 既存データに増分を追加
                  
        処理の流れ:
        1. 新規データ読み込み
        2. データ処理
        3. 既存データ読み込み（増分モードの場合）
        4. データマージ（増分モードの場合）
        5. 結果保存
        """
        # 新規データ読み込み
        new_data = self.load_raw_stock_data(ticker, mode)
        
        if new_data.empty:
            logger.warning(f"マージする新規データなし: {ticker}")
            return
        
        # データ処理
        processed_data = self.process_stock_data(ticker, new_data)
        
        if processed_data.empty:
            logger.warning(f"処理済みデータなし: {ticker}")
            return
        
        # ティッカーからファイル名を取得
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        # 既存のファイルパス
        existing_file_path = self.processed_data_path / "prices" / f"{file_ticker}.parquet"
        
        # 既存データとのマージ
        if existing_file_path.exists() and mode == "incr":
            try:
                # 既存データ読み込みと結合
                existing_data = pd.read_parquet(existing_file_path)
                combined_data = pd.concat([existing_data, processed_data])
                
                # 重複インデックスの削除
                if combined_data.index.duplicated().any():
                    combined_data = combined_data[~combined_data.index.duplicated(keep="last")]
                    
                # 日付順にソート
                combined_data = combined_data.sort_index()
                
                # マージデータ保存
                self.save_processed_data(ticker, combined_data)
                logger.info(f"データマージ完了: {ticker} (既存 + 新規)")
                
            except Exception as e:
                logger.error(f"データマージエラー ({ticker}): {e}")
                # エラー時は新規データだけ保存
                self.save_processed_data(ticker, processed_data)
                
        else:
            # 既存データがない場合は新規データをそのまま保存
            self.save_processed_data(ticker, processed_data)
            logger.info(f"新規データ保存完了: {ticker}")
    
    def create_features(self, ticker: str, lookback_periods: List[int] = [5, 10, 20, 60]) -> pd.DataFrame:
        """
        特徴量生成
        
        Args:
            ticker: 銘柄コード
            lookback_periods: テクニカル指標の計算期間のリスト
            
        Returns:
            特徴量データ
            
        生成される特徴量:
        - 過去のリターン（日次、週次、月次）
        - 移動平均（単純移動平均、指数移動平均）
        - ボラティリティ（標準偏差）
        - RSI（相対力指数）
        - MACD（移動平均収束拡散）
        - 翌日の上昇/下落フラグ（目標変数）
        """
        # ティッカーからファイル名を取得
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        # 処理済みデータのファイルパス
        file_path = self.processed_data_path / "prices" / f"{file_ticker}.parquet"
        
        if not file_path.exists():
            logger.warning(f"処理済みデータがありません: {file_path}")
            return pd.DataFrame()
        
        try:
            df = pd.read_parquet(file_path)
            
            if df.empty:
                return pd.DataFrame()
            
            features = df.copy()
            
            # リターン計算
            features["Daily_Return"] = features["Close"].pct_change()
            
            # 各期間のリターン
            for period in lookback_periods:
                features[f"Return_{period}d"] = features["Close"].pct_change(period)
            
            # 移動平均
            for period in lookback_periods:
                features[f"MA_{period}d"] = features["Close"].rolling(window=period).mean()
                
            # 移動平均からの乖離率
            for period in lookback_periods:
                features[f"MA_Deviation_{period}d"] = (features["Close"] / features[f"MA_{period}d"] - 1) * 100
            
            # ボラティリティ（標準偏差）
            for period in lookback_periods:
                features[f"Volatility_{period}d"] = features["Daily_Return"].rolling(window=period).std() * (252 ** 0.5)  # 年率換算
            
            # RSI
            for period in lookback_periods:
                delta = features["Close"].diff()
                gain = delta.where(delta > 0, 0)
                loss = -delta.where(delta < 0, 0)
                avg_gain = gain.rolling(window=period).mean()
                avg_loss = loss.rolling(window=period).mean()
                rs = avg_gain / avg_loss
                features[f"RSI_{period}d"] = 100 - (100 / (1 + rs))
            
            # MACD
            features["MACD"] = features["Close"].ewm(span=12).mean() - features["Close"].ewm(span=26).mean()
            features["MACD_Signal"] = features["MACD"].ewm(span=9).mean()
            features["MACD_Histogram"] = features["MACD"] - features["MACD_Signal"]
            
            # 欠損値の削除
            features = features.dropna()
            
            # 目標変数：翌日のリターン
            features["Next_Day_Return"] = features["Daily_Return"].shift(-1)
            features["Next_Day_Up"] = (features["Next_Day_Return"] > 0).astype(int)  # 2値分類用
            
            # 最終行の次日リターンは不明なので削除
            features = features.dropna(subset=["Next_Day_Return"])
            
            return features
            
        except Exception as e:
            logger.error(f"特徴量生成エラー ({ticker}): {e}")
            return pd.DataFrame()
    
    def save_features(self, ticker: str, data: pd.DataFrame):
        """
        特徴量データの保存
        
        Args:
            ticker: 銘柄コード
            data: 特徴量データ
        """
        if data.empty:
            logger.warning(f"保存する特徴量データなし: {ticker}")
            return
        
        # ティッカーからファイル名を取得
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        # 特徴量ディレクトリがなければ作成
        feature_dir = self.feature_data_path / "features"
        feature_dir.mkdir(parents=True, exist_ok=True)
            
        # ファイルパス設定
        file_path = feature_dir / f"{file_ticker}.parquet"
        
        # データ保存
        try:
            data.to_parquet(file_path)
            logger.info(f"特徴量データ保存完了: {file_path}")
        except Exception as e:
            logger.error(f"特徴量データ保存エラー ({file_path}): {e}")
            
    def process_and_create_features(self, ticker: str, mode: str = "full"):
        """
        データ処理と特徴量生成の一括実行
        
        Args:
            ticker: 銘柄コード
            mode: 処理モード ('full' または 'incr')
        """
        # マージ処理
        self.merge_stock_data(ticker, mode)
        
        # 特徴量生成
        features = self.create_features(ticker)
        
        if not features.empty:
            # 特徴量保存
            self.save_features(ticker, features)
            
        return features 
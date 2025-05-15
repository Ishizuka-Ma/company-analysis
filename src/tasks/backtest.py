"""
バックテスト実行モジュール

このモジュールは、機械学習モデルを用いた株価予測と取引戦略のバックテストを実行します。
株価データと生成された特徴量を使って取引戦略の有効性を検証し、
パフォーマンス指標を計算します。

主な機能:
- 機械学習モデル（RandomForest）の訓練と予測
- バックテスト用データの準備
- 取引戦略のシミュレーション実行
- パフォーマンス指標の計算と結果の可視化
- バックテスト結果の保存
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from enum import Enum

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# ロガーの設定
from src.utils.log_config import logger

class TradingDays(Enum):
    ONE_YEAR = 252
    FOUR_YEARS = 252 * 4

class Threshold(Enum):
    BINARY_CLASSIFICATION = 0.5

class MLStrategy(Strategy):
    """
    機械学習モデルを使用した取引戦略
    
    機械学習モデルの予測結果に基づいて売買判断を行うバックテスト戦略クラスです。
    予測値が閾値（0.5）を上回る場合は買い、下回る場合は売りと判断します。
    
    Attributes:
        n_train (int): 訓練データの期間（日数）
        predict_col (str): 予測値の列名
    """
    # パラメータ
    n_train = TradingDays.FOUR_YEARS.value
    predict_col = "prediction"
    
    def init(self):
        # 予測値を取得
        self.predictions = self.data[self.predict_col]
    
    def next(self):
        """
        次のステップでの取引判断
        
        バックテストの各ステップ（日）ごとに呼び出され、
        予測値に基づいて売買判断を行います。
        
        ルール:
        - 予測値 > 0.5: 上昇予測 → 買いポジション
        - 予測値 <= 0.5: 下落予測 → 売りポジション（または保有なし）
        """
        if self.predictions[-1] > Threshold.BINARY_CLASSIFICATION.value:
            # 上昇予測なら全額買い
            if not self.position:
                self.buy()
        else:
            # 下落予測なら保有していれば売り
            if self.position:
                self.position.close()

class BackTester:
    """
    バックテスト実行クラス
    
    機械学習モデルを訓練し、その予測に基づく取引戦略をバックテストします。
    結果の統計情報とグラフを生成し、ファイルに保存します。
    
    Attributes:
        feature_data_path (Path): 特徴量データの読み込み元ディレクトリ
        output_path (Path): 結果の保存先ディレクトリ
    """
    
    def __init__(self, feature_data_path: str):
        """
        初期化
        
        Args:
            feature_data_path: 特徴量データパス
            
        初期化処理の流れ:
        1. 引数で受け取ったパスをPathオブジェクトに変換
        2. 出力ディレクトリを作成
        """
        self.feature_data_path = Path(feature_data_path)
        
        self.output_path = Path("./output")
        self.output_path.mkdir(parents=True, exist_ok=True)
    
    def load_feature_data(self, ticker: str) -> pd.DataFrame:
        """
        特徴量データ読み込み
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            特徴量データ（DataFrame）
            
        特徴量データファイルが存在しない場合や読み込みエラー時は、
        空のDataFrameを返します。
        """
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        file_path = self.feature_data_path / "features" / f"{file_ticker}.parquet"
        
        if not file_path.exists():
            logger.warning(f"特徴量データが存在しません: {file_path}")
            return pd.DataFrame()
        
        # データ読み込み
        try:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            logger.error(f"特徴量データ読み込みエラー ({file_path}): {e}")
            return pd.DataFrame()
    
    def train_model(self, data: pd.DataFrame, test_size: float = 0.2, random_state: int = 42) -> Tuple:
        """
        機械学習モデルの訓練
        
        Args:
            data: 特徴量データ
            test_size: テストデータの割合
            random_state: 乱数シード
            
        Returns:
            モデル、特徴量列名、X_train, X_test, y_train, y_test のタプル
            
        処理の流れ:
        1. 特徴量と目標変数の抽出
        2. 訓練データとテストデータに時系列分割
        3. RandomForestClassifierモデルの訓練
        4. 訓練済みモデルと関連データの返却
        """
        if data.empty:
            return None, [], None, None, None, None
        
        # 特徴量と目標変数の設定
        target_col = "Next_Day_Up"  # 翌日上昇=1, 下落=0
        
        # 特徴量として使用する列を選択
        feature_cols = [col for col in data.columns if col.startswith(("Return", "MA", "Volatility", "RSI", "MACD"))]
        
        # 特徴量と目標変数を抽出
        X = data[feature_cols].copy()
        y = data[target_col].copy()
        
        # 訓練データとテストデータに分割
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, shuffle=False
        )
        
        # モデルの訓練
        model = RandomForestClassifier(n_estimators=100, random_state=random_state)
        model.fit(X_train, y_train)
        
        return model, feature_cols, X_train, X_test, y_train, y_test
    
    def create_backtest_data(self, data: pd.DataFrame, model, feature_cols: List[str]) -> pd.DataFrame:
        """
        バックテスト用データの作成
        
        Args:
            data: 特徴量データ
            model: 訓練済みモデル
            feature_cols: 特徴量の列名リスト
            
        Returns:
            バックテスト用データ
            
        処理内容:
        1. 訓練済みモデルで株価上昇確率を予測
        2. 予測結果をデータフレームに追加
        3. バックテストに必要な列（OHLCV + 予測値）だけを抽出
        """
        if data.empty or model is None:
            return pd.DataFrame()
        
        # バックテスト用データの作成
        backtest_data = data.copy()
        
        # 予測実行
        predictions = model.predict_proba(backtest_data[feature_cols])[:, 1]  # 上昇確率を取得
        backtest_data["prediction"] = predictions
        
        # バックテスト用のデータフレーム作成
        bt_data = backtest_data[["Open", "High", "Low", "Close", "Volume", "prediction"]].copy()
        
        return bt_data
    
    def run_backtest(self, data: pd.DataFrame, cash: int = 1000000, commission: float = 0.001) -> Dict:
        """
        バックテスト実行
        
        Args:
            data: バックテスト用データ
            cash: 初期資金
            commission: 取引手数料
            
        Returns:
            バックテスト結果を含む辞書
            
        計算される主なパフォーマンス指標:
        - リターン: 総収益率
        - 最大ドローダウン: 最大下落率
        - 取引回数: 売買の総回数
        - 勝率: 利益が出た取引の割合
        - シャープレシオ: リスク調整後のリターン
        - ソルティノレシオ: 下方リスク調整後のリターン
        - SQN: システム品質指数
        - カルマーレシオ: ドローダウンあたりのリターン
        """
        if data.empty:
            return {"error": "データが空です"}
        
        try:
            # バックテスト実行
            bt = Backtest(data, MLStrategy, cash=cash, commission=commission)
            result = bt.run()
            
            # 結果の整形
            backtest_result = {
                "Return": result["Return"] * 100,  # パーセント表示
                "Max Drawdown": result["Max. Drawdown"] * 100,  # パーセント表示
                "# Trades": result["# Trades"],
                "Win Rate": result["Win Rate"] * 100,  # パーセント表示
                "Sharpe Ratio": result["Sharpe Ratio"],
                "Sortino Ratio": result["Sortino Ratio"],
                "SQN": result["SQN"],
                "Calmar Ratio": result["Calmar Ratio"],
            }
            
            return {
                "stats": backtest_result,
                "full_result": result,
                "backtest": bt
            }
            
        except Exception as e:
            logger.error(f"バックテスト実行エラー: {e}")
            return {"error": str(e)}
    
    def plot_backtest_result(self, backtest_result: Dict, ticker: str):
        """
        バックテスト結果のプロット
        
        Args:
            backtest_result: バックテスト結果
            ticker: 銘柄コード
            
        生成されるグラフ:
        1. 取引戦略のエクイティカーブ
        2. 主要パフォーマンス指標の棒グラフ
        
        グラフは output_path/plots ディレクトリに保存されます。
        """
        if "error" in backtest_result:
            logger.error(f"プロットエラー: {backtest_result['error']}")
            return
        
        try:
            if "." in ticker:
                file_ticker = ticker.split(".")[0]
            else:
                file_ticker = ticker
                
            # 保存先パス
            plot_dir = self.output_path / "plots"
            plot_dir.mkdir(parents=True, exist_ok=True)
            
            # プロット生成
            bt = backtest_result["backtest"]
            fig, _ = bt.plot(resample=False, open_browser=False)
            
            # プロット保存
            fig_path = plot_dir / f"{file_ticker}_backtest.png"
            fig.savefig(fig_path)
            
            # 統計情報のプロット
            stats = backtest_result["stats"]
            
            # 新しい図を作成
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # 統計情報をプロット
            stats_to_plot = {k: v for k, v in stats.items() if k not in ["# Trades"]}
            bars = ax.bar(list(stats_to_plot.keys()), list(stats_to_plot.values()))
            
            # バーに値を表示
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{height:.2f}', ha='center', va='bottom')
            
            ax.set_title(f"{file_ticker} バックテスト統計")
            ax.set_ylabel("値")
            
            # 統計プロット保存
            stats_fig_path = plot_dir / f"{file_ticker}_stats.png"
            fig.savefig(stats_fig_path)
            
            logger.info(f"バックテスト結果プロット保存完了: {fig_path}, {stats_fig_path}")
            
        except Exception as e:
            logger.error(f"バックテスト結果プロットエラー ({ticker}): {e}")
    
    def save_backtest_result(self, backtest_result: Dict, ticker: str):
        """
        バックテスト結果の保存
        
        Args:
            backtest_result: バックテスト結果
            ticker: 銘柄コード
            
        バックテスト結果をJSON形式で output_path/results ディレクトリに保存します。
        保存される情報には、各種パフォーマンス指標が含まれます。
        """
        if "error" in backtest_result:
            logger.error(f"結果保存エラー: {backtest_result['error']}")
            return
        
        try:
            # ティッカーからファイル名を取得
            if "." in ticker:
                file_ticker = ticker.split(".")[0]
            else:
                file_ticker = ticker
                
            # 保存先パス
            result_dir = self.output_path / "results"
            result_dir.mkdir(parents=True, exist_ok=True)
            
            # 保存するデータ
            save_data = backtest_result["stats"]
            
            # JSONで保存
            result_path = result_dir / f"{file_ticker}_result.json"
            with open(result_path, "w", encoding="utf-8") as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"バックテスト結果保存完了: {result_path}")
            
        except Exception as e:
            logger.error(f"バックテスト結果保存エラー ({ticker}): {e}")
    
    def run_full_backtest(self, ticker: str, test_size: float = 0.3, cash: int = 1000000, commission: float = 0.001):
        """
        バックテストの全工程を実行
        
        Args:
            ticker: 銘柄コード
            test_size: テストデータの割合
            cash: 初期資金
            commission: 取引手数料
            
        Returns:
            バックテスト結果と統計情報
            
        実行の流れ:
        1. 特徴量データの読み込み
        2. 機械学習モデルの訓練
        3. バックテスト用データの作成
        4. バックテスト実行
        5. 結果のプロットと保存
        """
        logger.info(f"バックテスト開始: {ticker}")
        
        try:
            # 特徴量データ読み込み
            feature_data = self.load_feature_data(ticker)
            
            if feature_data.empty:
                error_msg = f"特徴量データが空です: {ticker}"
                logger.error(error_msg)
                return {"error": error_msg}
            
            # モデル訓練
            model, feature_cols, _, _, _, _ = self.train_model(feature_data, test_size)
            
            if model is None:
                error_msg = f"モデル訓練失敗: {ticker}"
                logger.error(error_msg)
                return {"error": error_msg}
            
            # バックテスト用データ作成
            backtest_data = self.create_backtest_data(feature_data, model, feature_cols)
            
            if backtest_data.empty:
                error_msg = f"バックテスト用データが空です: {ticker}"
                logger.error(error_msg)
                return {"error": error_msg}
            
            # バックテスト実行
            result = self.run_backtest(backtest_data, cash, commission)
            
            if "error" in result:
                return result
            
            # 結果のプロットと保存
            self.plot_backtest_result(result, ticker)
            self.save_backtest_result(result, ticker)
            
            logger.info(f"バックテスト完了: {ticker}")
            
            return result
            
        except Exception as e:
            error_msg = f"バックテスト実行エラー ({ticker}): {e}"
            logger.error(error_msg)
            return {"error": error_msg} 
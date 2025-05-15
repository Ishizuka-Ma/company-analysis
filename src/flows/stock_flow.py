"""
株価データ取得・バックテストのPrefectフロー

このモジュールでは、Prefectフレームワークを使用して株価データの取得、処理、
バックテスト実行までの一連のワークフローを定義しています。
各タスクを連携させ、データパイプラインを構築します。

主な機能:
- 企業リスト取得タスク
- 株価データ取得タスク
- データ処理・特徴量生成タスク
- バックテスト実行タスク
- 単一銘柄、複数銘柄、市場ベースのバックテストフロー
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from prefect import flow, task, get_run_logger

from src.utils.log_config import logger
from src.tasks.data_fetcher import StockDataFetcher
from src.tasks.data_processor import StockDataProcessor
from src.tasks.backtest import BackTester
from src.tasks.listing_checker import StockListingChecker
from src.tasks.split_adjustment import SplitAdjustment

# プロジェクトルートパスの取得
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# データパス設定
DATA_PATH = os.environ.get(os.path.join(ROOT_DIR, "data"))
RAW_DATA_PATH = os.environ.get(os.path.join(DATA_PATH, "raw"))
PROCESSED_DATA_PATH = os.environ.get(os.path.join(DATA_PATH, "processed"))
FEATURE_DATA_PATH = os.environ.get(os.path.join(DATA_PATH, "feature"))
META_DATA_PATH = os.environ.get(os.path.join(DATA_PATH, "meta"))
OUTPUT_PATH = os.environ.get(os.path.join(ROOT_DIR, "output"))

@task(name="企業リスト取得")
def fetch_company_list(market: str = "ALL") -> pd.DataFrame:
    """
    上場企業リストの取得タスク
    
    指定された市場区分の上場企業リストを取得します。
    実際のプロジェクトでは、東証からダウンロードしたExcelファイルを
    解析する処理が実装されます。
    
    Args:
        market: 市場区分 ("ALL", "プライム", "スタンダード", "グロース")
        
    Returns:
        企業リストのDataFrame（コード、銘柄名、市場区分を含む）
        
    Note:
        このタスクはPrefectのタスクとして登録され、実行ログが記録されます。
    """
    logger = get_run_logger()
    logger.info(f"企業リスト取得開始: {market}市場")
    
    try:
        # データ取得クラスのインスタンス化
        data_fetcher = StockDataFetcher(
            meta_file_path=os.path.join(META_DATA_PATH, "latest_date.json"),
            raw_data_path=RAW_DATA_PATH
        )
        
        # 企業リスト取得
        companies = data_fetcher.fetch_company_list(market)
        
        logger.info(f"企業リスト取得完了: {len(companies)}社")
        return companies
    
    except Exception as e:
        logger.error(f"企業リスト取得エラー: {e}")
        return pd.DataFrame()

@task(name="株価データ取得")
def fetch_stock_data(ticker: str, full_load: bool) -> bool:
    """
    株価データ取得タスク
    
    指定された銘柄の株価データをyfinanceから取得し、ローカルに保存します。
    full_loadがTrueの場合は全期間データを取得し、
    Falseの場合は前回取得時からの差分データのみを取得します。
    
    Args:
        ticker: 銘柄コード
        full_load: 全期間取得フラグ
        
    Returns:
        処理成功フラグ（True/False）
        
    Note:
        結果はRAW_DATA_PATH以下に保存されます。
    """
    logger = get_run_logger()
    logger.info(f"株価データ取得開始: {ticker} (full_load={full_load})")
    
    try:
        # データ取得クラスのインスタンス化
        data_fetcher = StockDataFetcher(
            meta_file_path=os.path.join(META_DATA_PATH, "latest_date.json"),
            raw_data_path=RAW_DATA_PATH
        )
        
        # 株価データ取得・保存
        data_fetcher.fetch_and_save_stock_data(ticker, full_load)
        
        logger.info(f"株価データ取得完了: {ticker}")
        return True
    
    except Exception as e:
        logger.error(f"株価データ取得エラー ({ticker}): {e}")
        return False

@task(name="データ処理・特徴量生成")
def process_stock_data(ticker: str, mode: str = "full") -> bool:
    """
    株価データ処理と特徴量生成タスク
    
    取得した株価データを処理し、バックテスト用の特徴量を生成します。
    指定されたモードに応じて、全データの再処理または増分データの
    処理を行います。
    
    Args:
        ticker: 銘柄コード
        mode: 処理モード ('full'=全期間処理 または 'incr'=増分処理)
        
    Returns:
        処理成功フラグ（True/False）
        
    処理の流れ:
    1. 生データの読み込み
    2. データクレンジングと標準化
    3. 既存データとのマージ
    4. テクニカル指標などの特徴量生成
    5. 特徴量データの保存
    """
    logger = get_run_logger()
    logger.info(f"データ処理開始: {ticker} (mode={mode})")
    
    try:
        # データ処理クラスのインスタンス化
        data_processor = StockDataProcessor(
            raw_data_path=RAW_DATA_PATH,
            processed_data_path=PROCESSED_DATA_PATH,
            feature_data_path=FEATURE_DATA_PATH
        )
        
        # データ処理と特徴量生成
        features = data_processor.process_and_create_features(ticker, mode)
        
        success = not features.empty
        
        if success:
            logger.info(f"データ処理・特徴量生成完了: {ticker}")
        else:
            logger.warning(f"データ処理結果が空: {ticker}")
            
        return success
    
    except Exception as e:
        logger.error(f"データ処理エラー ({ticker}): {e}")
        return False

@task(name="バックテスト実行")
def run_backtest(ticker: str, test_size: float = 0.3, cash: int = 1000000, commission: float = 0.001) -> Dict:
    """
    バックテスト実行タスク
    
    生成された特徴量を使用して機械学習モデルを訓練し、
    バックテストを実行します。結果はファイルに保存され、
    パフォーマンス指標などの統計情報を返します。
    
    Args:
        ticker: 銘柄コード
        test_size: テストデータの割合（0〜1の小数）
        cash: バックテストの初期資金
        commission: 取引手数料（割合）
        
    Returns:
        バックテスト結果を含む辞書
        
    生成される主な指標:
    - リターン: 総収益率（%）
    - 最大ドローダウン: 最大下落率（%）
    - 取引回数: 売買の総回数
    - 勝率: 利益が出た取引の割合（%）
    - シャープレシオ: リスク調整後のリターン
    """
    logger = get_run_logger()
    logger.info(f"バックテスト開始: {ticker}")
    
    try:
        # バックテストクラスのインスタンス化
        backtester = BackTester(
            feature_data_path=FEATURE_DATA_PATH,
            output_path=OUTPUT_PATH
        )
        
        # バックテスト実行
        result = backtester.run_full_backtest(ticker, test_size, cash, commission)
        
        if "error" in result:
            logger.error(f"バックテスト失敗: {ticker} - {result['error']}")
            return result
        
        logger.info(f"バックテスト完了: {ticker}")
        
        # 結果のサマリーをログ出力
        stats = result["stats"]
        logger.info(f"バックテスト結果 ({ticker}):")
        logger.info(f"  リターン: {stats['Return']:.2f}%")
        logger.info(f"  最大ドローダウン: {stats['Max Drawdown']:.2f}%")
        logger.info(f"  取引回数: {stats['# Trades']}")
        logger.info(f"  勝率: {stats['Win Rate']:.2f}%")
        logger.info(f"  シャープレシオ: {stats['Sharpe Ratio']:.2f}")
        
        return result
    
    except Exception as e:
        logger.error(f"バックテスト実行エラー ({ticker}): {e}")
        return {"error": str(e)}

@flow(name="単一銘柄バックテストフロー")
def stock_backtest_flow(
    ticker: str,
    full_load: bool = False,
    test_size: float = 0.3,
    cash: int = 1000000,
    commission: float = 0.001
) -> Dict:
    """
    単一銘柄のバックテストフロー
    
    1つの銘柄に対するデータ取得から処理、バックテスト実行までの
    一連のフローを定義します。
    
    Args:
        ticker: 銘柄コード
        full_load: 全期間取得フラグ
        test_size: テストデータの割合
        cash: 初期資金
        commission: 取引手数料
        
    Returns:
        バックテスト結果
        
    フローの流れ:
    1. 株価データ取得（全期間または差分）
    2. データ処理・特徴量生成
    3. バックテスト実行
    """
    logger = get_run_logger()
    logger.info(f"バックテストフロー開始: {ticker}")
    
    # 株価データ取得
    fetch_success = fetch_stock_data(ticker, full_load)
    
    if not fetch_success:
        return {"error": "株価データ取得失敗"}
    
    # データ処理・特徴量生成
    process_success = process_stock_data(ticker, "full" if full_load else "incr")
    
    if not process_success:
        return {"error": "データ処理失敗"}
    
    # バックテスト実行
    result = run_backtest(ticker, test_size, cash, commission)
    
    logger.info(f"バックテストフロー完了: {ticker}")
    
    return result

@flow(name="複数銘柄バックテストフロー")
def multi_stock_backtest_flow(
    tickers: List[str],
    full_load: bool = False,
    test_size: float = 0.3,
    cash: int = 1000000,
    commission: float = 0.001
) -> Dict:
    """
    複数銘柄のバックテストフロー
    
    複数の銘柄それぞれに対してバックテストを実行し、
    結果をまとめて返します。
    
    Args:
        tickers: 銘柄コードのリスト
        full_load: 全期間取得フラグ
        test_size: テストデータの割合
        cash: 初期資金
        commission: 取引手数料
        
    Returns:
        銘柄ごとのバックテスト結果の辞書
        
    フローの特徴:
    - 各銘柄ごとに独立してバックテストを実行
    - 一部の銘柄で失敗しても処理を継続
    - すべての結果を辞書形式でまとめて返却
    """
    logger = get_run_logger()
    logger.info(f"複数銘柄バックテストフロー開始: {len(tickers)}銘柄")
    
    results = {}
    
    for ticker in tickers:
        # 各銘柄のバックテストを実行
        result = stock_backtest_flow(
            ticker=ticker,
            full_load=full_load,
            test_size=test_size,
            cash=cash,
            commission=commission
        )
        
        results[ticker] = result
    
    logger.info(f"複数銘柄バックテストフロー完了: {len(tickers)}銘柄")
    
    return results

@flow(name="市場ベースバックテストフロー")
def market_backtest_flow(
    market: str = "ALL",
    full_load: bool = False,
    test_size: float = 0.3,
    cash: int = 1000000,
    commission: float = 0.001
) -> Dict:
    """
    市場ベースバックテストフロー
    
    指定された市場区分の全銘柄に対してバックテストを実行します。
    企業リストを取得し、その中の銘柄それぞれに対してバックテストを行います。
    
    Args:
        market: 市場区分 ("ALL", "プライム", "スタンダード", "グロース")
        full_load: 全期間取得フラグ
        test_size: テストデータの割合
        cash: 初期資金
        commission: 取引手数料
        
    Returns:
        銘柄ごとのバックテスト結果の辞書
        
    フローの流れ:
    1. 企業リスト取得
    2. 取得した企業リストの銘柄コードを抽出
    3. 複数銘柄バックテストフローを実行
    """
    logger = get_run_logger()
    logger.info(f"市場ベースバックテストフロー開始: {market}市場")
    
    # 企業リスト取得
    companies = fetch_company_list(market)
    
    if companies.empty:
        return {"error": "企業リスト取得失敗"}
    
    # 銘柄コードリスト取得
    tickers = companies["コード"].tolist()
    
    logger.info(f"対象銘柄数: {len(tickers)}銘柄")
    
    # 複数銘柄バックテストフロー実行
    results = multi_stock_backtest_flow(
        tickers=tickers,
        full_load=full_load,
        test_size=test_size,
        cash=cash,
        commission=commission
    )
    
    logger.info(f"市場ベースバックテストフロー完了: {market}市場")
    
    return results
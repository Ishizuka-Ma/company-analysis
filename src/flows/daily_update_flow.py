"""
毎日の株価データ更新フロー

このモジュールでは、以下の3つの処理を毎日実行するPrefectフローを定義します：
1. 新規上場と廃止に関する情報の更新
2. 株価データの取得
3. 株式分割と株式合併による過去の株価データの修正

このフローはGitHub Actionsから自動実行されるように設定されています。

このモジュールは日次データ更新の主要な実装を提供します。各タスク間の依存関係を
明確にし、前段階の処理が失敗した場合は後続の処理をスキップする仕組みを
実装しています。
"""
from prefect import flow, task, get_run_logger
from src.tasks.listing_checker import StockListingChecker
from src.tasks.data_fetcher import StockDataFetcher
from src.tasks.split_adjustment import SplitAdjustment
import os
from pathlib import Path

# プロジェクトルートパスの取得
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# データパス設定
DATA_PATH = os.path.join(ROOT_DIR, "data")
RAW_DATA_PATH = os.path.join(DATA_PATH, "raw")
PROCESSED_DATA_PATH = os.path.join(DATA_PATH, "processed")
META_DATA_PATH = os.path.join(DATA_PATH, "meta")

@task(name="上場情報更新タスク")
def update_listing_info():
    """新規上場と廃止に関する情報を更新するタスク"""
    logger = get_run_logger()
    logger.info("上場銘柄情報の更新開始")
    
    try:
        # 上場情報チェッカーのインスタンス化
        listing_checker = StockListingChecker(processed_data_dir=PROCESSED_DATA_PATH)
        
        # 上場情報の更新実行
        listing_checker.update_listing_data()
        
        logger.info("上場情報更新処理が完了しました")
        return True
    except Exception as e:
        logger.error(f"上場情報更新処理でエラーが発生しました: {e}")
        return False

@task(name="株価データ取得タスク")
def fetch_stock_data():
    """最新の株価データを取得するタスク"""
    logger = get_run_logger()
    logger.info("株価データ取得処理を開始します")
    
    try:
        # 上場企業リストの取得
        listing_checker = StockListingChecker(processed_data_dir=PROCESSED_DATA_PATH)
        company_data = listing_checker.load_existing_data()
        
        if company_data.empty:
            logger.warning("上場企業データが空です")
            return False
        
        # データ取得クラスのインスタンス化
        data_fetcher = StockDataFetcher(
            meta_file_path=os.path.join(META_DATA_PATH, "latest_date.json"),
            raw_data_path=RAW_DATA_PATH
        )
        
        # 全銘柄の株価データを取得
        success_count = 0
        ticker_codes = company_data["コード"].tolist()
        
        for ticker in ticker_codes:
            try:
                # 差分更新モードで株価データ取得
                data_fetcher.fetch_and_save_stock_data(ticker, full_load=False)
                success_count += 1
            except Exception as e:
                logger.error(f"銘柄 {ticker} のデータ取得中にエラーが発生しました: {e}")
        
        logger.info(f"株価データ取得処理が完了しました: {success_count}/{len(ticker_codes)} 銘柄成功")
        return success_count > 0
    except Exception as e:
        logger.error(f"株価データ取得処理でエラーが発生しました: {e}")
        return False

@task(name="株式分割調整タスク")
def adjust_stock_splits():
    """株式分割と株式合併による過去の株価データを修正するタスク"""
    logger = get_run_logger()
    logger.info("株式分割調整処理を開始します")
    
    try:
        # 株式分割調整クラスのインスタンス化
        split_adjuster = SplitAdjustment(processed_data_dir=PROCESSED_DATA_PATH)
        
        # 株式分割調整の実行
        split_adjuster.update_split_adjustments()
        
        logger.info("株式分割調整処理が完了しました")
        return True
    except Exception as e:
        logger.error(f"株式分割調整処理でエラーが発生しました: {e}")
        return False

@flow(name="日次株価データ更新フロー")
def daily_update_flow():
    """
    上場情報更新、株価データ取得、株式分割調整を順番に実行するフロー
        
    新規上場・上場廃止の反映と株式分割調整を
    順番に実行します。
    
    Returns:
        フロー実行結果
    """
    logger = get_run_logger()
    logger.info("日次株価データ更新フローを開始します")
    
    # 1. 新規上場と廃止に関する情報の更新
    listing_result = update_listing_info()
    
    # 2. 株価データの取得
    if listing_result:
        fetch_result = fetch_stock_data()
    else:
        logger.warning("上場情報の更新に失敗したため、株価データの取得をスキップします")
        fetch_result = False
    
    # 3. 株式分割と株式合併による過去の株価データの修正
    if listing_result and fetch_result:
        adjust_result = adjust_stock_splits()
    else:
        logger.warning("新規上場と廃止に関する情報の更新もしくは株価データの取得に失敗したため、株式分割調整をスキップします")
        adjust_result = False
    
    # 処理結果の集計
    total_success = sum([listing_result, fetch_result, adjust_result])
    logger.info(f"日次株価データ更新フローが完了しました。成功タスク: {total_success}/3")
    
    return {
        "listing_update": listing_result,
        "stock_data_fetch": fetch_result,
        "split_adjustment": adjust_result,
        "overall_success": total_success == 3
    }

if __name__ == "__main__":
    # コマンドラインから直接実行時はフローを実行
    daily_update_flow() 
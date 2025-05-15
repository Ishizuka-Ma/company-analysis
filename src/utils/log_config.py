"""
ログ設定モジュール

このモジュールでは、アプリケーション全体で使用するロギング設定を提供します。
ログはlogフォルダに当日の日付を含むファイル名で保存されます。
"""
import logging
import os
from datetime import datetime
from pathlib import Path

# プロジェクトルートパスの取得
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# ログディレクトリ設定
LOG_DIR = os.environ.get(os.path.join(ROOT_DIR, "log"))

# ログファイル名（当日日付を含む）
LOG_FILENAME = f"{datetime.now().strftime('%Y-%m-%d')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILENAME)

def setup_logging(level=logging.INFO):
    """
    アプリケーション全体のログ設定をセットアップします
    
    Args:
        level: ログレベル（デフォルトはINFO）
        
    ログはコンソールと日付ベースのファイルの両方に出力されます。
    """
    # ログディレクトリが存在しない場合は作成
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    
    # ルートロガーの設定
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 既存のハンドラをクリア
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # コンソール出力用ハンドラ
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)
    
    # ファイル出力用ハンドラ
    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding='utf-8')
    file_handler.setLevel(level)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)
    
    root_logger.info(f"ログ設定を完了しました。ログファイル: {LOG_FILE_PATH}")
    
    return root_logger

# モジュールのインポート時に自動的にログ設定を行う
logger = setup_logging() 
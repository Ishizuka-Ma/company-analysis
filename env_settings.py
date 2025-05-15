"""
環境変数設定ファイル
"""
import os
from pathlib import Path

# プロジェクトのベースディレクトリ
BASE_DIR = Path(__file__).resolve().parent

# APIエンドポイント
API_URL = os.environ.get("API_URL", "http://localhost:8000")

# Prefect設定
PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://localhost:4200/api")
PREFECT_UI_API_URL = os.environ.get("PREFECT_UI_API_URL", "http://localhost:4200/api")

# データパス
DATA_PATH = os.environ.get("DATA_PATH", os.path.join(BASE_DIR, "data"))
RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", os.path.join(DATA_PATH, "raw"))
PROCESSED_DATA_PATH = os.environ.get("PROCESSED_DATA_PATH", os.path.join(DATA_PATH, "processed"))
FEATURE_DATA_PATH = os.environ.get("FEATURE_DATA_PATH", os.path.join(DATA_PATH, "feature"))
META_DATA_PATH = os.environ.get("META_DATA_PATH", os.path.join(DATA_PATH, "meta"))

# 実行設定
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
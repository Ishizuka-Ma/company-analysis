"""
FastAPI バックエンド

このモジュールでは、バックテストシステムのREST APIを提供します。
ブラウザやフロントエンドアプリケーションからのリクエストを受け付け、
Prefectワークフローの実行や結果の取得を行います。

主な機能:
- バックテスト実行エンドポイント（単一銘柄、複数銘柄、市場区分）
- フロー実行状態の確認
- バックテスト結果の取得と返却
- 上場情報・株式分割調整フローの実行
- CORS対応によるクロスオリジンリクエストの許可
"""
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Union
from enum import Enum
import logging

import httpx
# FastAPIフレームワーク
# - FastAPI: Webアプリケーションフレームワーク本体
# - HTTPException: エラーレスポンスを返すための例外クラス
# - Query: クエリパラメータのバリデーションと型変換を行うためのユーティリティ
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
import uvicorn

# CORSミドルウェア
# - CORSMiddleware: クロスオリジンリソース共有(CORS)を有効にするためのミドルウェア
# - 異なるオリジン(ドメイン)からのAPIリクエストを許可できる
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel # データのバリデーションや型ヒントを提供

from src.utils.log_config import logger
from src.flows.stock_flow import (
    market_backtest_flow,
    multi_stock_backtest_flow,
    stock_backtest_flow,
    listing_split_flow,
)

# プロジェクトルートパスの取得
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# 出力パス設定
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", os.path.join(ROOT_DIR, "output"))

# Prefect API設定
PREFECT_API_URL = os.environ.get("http://localhost:4200/api") # デフォルトを指定(http://localhost:4200)

# FastAPIアプリケーション
app = FastAPI(
    title="株式取引戦略のバックテストAPI", # OpenAPIおよび自動APIドキュメントUIでAPIのタイトル/名前として使用される
)

"""
CORS設定

allow_headers 
- "Authorization": 認証トークンをリクエストヘッダーで送信できるようにする
- "Content-Type": リクエストのデータ形式を指定できるようにする
"""
# 本番環境
origins = [
    # "http://localhost:3000",  # React/Next.js開発サーバー
    "http://localhost:8501",  # Streamlit UI
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # 許可するオリジンを明示的に指定
    allow_credentials=True, # クッキーや認証情報付きリクエストを許可
    allow_methods=["GET", "POST"], # 必要なメソッドのみ許可（例: GET, POST）
    allow_headers=["Authorization", "Content-Type"], # 必要なヘッダーのみ許可
)

# ─── 列挙型の定義 ──────────────────────────────────────────────────────────
class Market(str, Enum):
    ALL        = "ALL"
    PRIME      = "プライム"
    STANDARD   = "スタンダード"
    GROWTH     = "グロース"

class FullLoad(Enum):
    FULL    = True
    PARTIAL = False  # デフォルト

class TestSize(float, Enum):
    TS_10 = 0.1
    TS_20 = 0.2 # デフォルト
    TS_30 = 0.3

class Cash(int, Enum):
    C10K  = 10000
    C100K = 100000
    C1M = 1000000 # デフォルト
    
class Commission(float, Enum):
    LOW    = 0.0005
    NORMAL = 0.001 # デフォルト
    HIGH   = 0.002
    
class WaitForCompletion(Enum):
    WAIT    = True
    CONTINUE = False  # デフォルト

# ─── バックテスト用モデル ───────────────────────────────────────────────
class BacktestRequest(BaseModel):
    """
    バックテストリクエストモデル
    
    複数銘柄のバックテスト実行リクエストに使用されるデータモデル。
    銘柄リストと各種パラメータを含みます。
    
    Attributes:
        tickers: 銘柄コードのリスト
        full_load: 全期間取得フラグ
        test_size: テストデータの割合
        cash: 初期資金
        commission: 取引手数料
    """
    tickers: List[str]
    full_load: bool = FullLoad.PARTIAL.value
    test_size: float = TestSize.TS_20.value
    cash: float = Cash.C1M.value
    commission: float = Commission.NORMAL.value

class MarketBacktestRequest(BaseModel):
    """
    市場バックテストリクエストモデル
    
    市場区分ごとのバックテスト実行リクエストに使用されるデータモデル。
    市場区分と各種パラメータを含みます。
    
    Attributes:
        market: 市場区分（"ALL"、"プライム"、"スタンダード"、"グロース"）
        full_load: 全期間取得フラグ
        test_size: テストデータの割合
        cash: 初期資金
        commission: 取引手数料
    """
    
    market: str = Market.ALL.value
    full_load: bool = FullLoad.PARTIAL.value
    test_size: float = TestSize.TS_20.value
    cash: float = Cash.C1M.value
    commission: float = Commission.NORMAL.value

# レスポンスモデル
class BacktestResult(BaseModel):
    """
    バックテスト結果モデル
    
    バックテスト実行結果を返すためのデータモデル。
    フロー実行IDやステータス、結果データを含みます。
    
    Attributes:
        flow_run_id: Prefectフロー実行ID
        status: 実行状態（"COMPLETED", "RUNNING", "FAILED"など）
        results: バックテスト結果データ
        error: エラーメッセージ（エラー発生時のみ）
    """
    
    flow_run_id: Optional[str] = None
    status: str
    results: Optional[Dict] = None
    error: Optional[str] = None

# 上場情報・株式分割フロー実行結果モデル
class ListingSplitResult(BaseModel):
    """
    上場情報・株式分割フロー実行結果モデル
    """
    flow_run_id: str
    status: str
    results: Optional[Dict] = None
    error: Optional[str] = None

# ヘルスチェック
@app.get("/health")
async def health_check():
    """
    ヘルスチェックエンドポイント
    
    APIサーバーの稼働状態を確認するためのシンプルなエンドポイント。
    監視システムやロードバランサーからの監視に使用します。
    
    Returns:
        {"status": "healthy"} - APIが正常動作中
    """
    return {"status": "healthy"}

# Prefectフロー実行リクエスト送信
async def run_prefect_flow(
    flow_name: str, parameters: Dict, flow_run_name: Optional[str] = None
) -> str:
    """
    Prefectフロー実行リクエスト送信
    
    Prefect APIを呼び出してフローの実行をリクエストする非同期関数。
    
    Args:
        flow_name: 実行するフロー名
        parameters: フロー実行時に渡すパラメータ
        flow_run_name: フロー実行に付ける名前（オプション）
        
    Returns:
        フロー実行ID（UUID文字列）
        
    Raises:
        HTTPException: Prefect APIリクエスト失敗時
    """
    url = f"{PREFECT_API_URL}/flows/name/{flow_name}/runs"
    
    payload = {
        "parameters": parameters,
    }
    
    if flow_run_name:
        payload["name"] = flow_run_name
    
    # 非同期HTTPクライアントを使用
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=payload)
    
    # Prefect APIの仕様によっては201「Created」以外（例: 200「OK」や202「Accepted」）
    # が返る場合もあるため、より柔軟に成功ステータスを判定
    if response.status_code not in (200, 201, 202):
        raise HTTPException(
            status_code=502,  # 外部API連携失敗時は502 Bad Gatewayが推奨される
            detail=f"Prefectフロー実行リクエスト送信失敗: {response.text}",
        )
    
    response_data = response.json()
    return response_data["id"]

# Prefectフロー実行状態取得
async def get_flow_run_status(flow_run_id: str) -> Dict:
    """
    Prefectフロー実行状態取得
    
    Prefect APIを呼び出して実行中のフロー状態を取得する非同期関数。
    
    Args:
        flow_run_id: フロー実行ID
        
    Returns:
        フロー実行状態を含む辞書
        
    Raises:
        HTTPException: Prefect APIリクエスト失敗時
    """
    url = f"{PREFECT_API_URL}/flow_runs/{flow_run_id}"
    
    # 非同期HTTPクライアントを使用
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    
    if response.status_code not in (200, 201, 202):
        raise HTTPException(
            status_code=502,  # 外部API連携失敗時は502 Bad Gatewayが推奨される
            detail=f"Prefectフロー実行状態取得失敗: {response.text}",
        )
    
    return response.json()

# バックテスト結果取得
def get_backtest_results(tickers: List[str]) -> Dict:
    """
    バックテスト結果取得
    
    指定された銘柄のバックテスト結果ファイルを読み込み、
    結果データを返します。
    
    Args:
        tickers: 銘柄コードのリスト
        
    Returns:
        銘柄ごとのバックテスト結果を含む辞書
        
    Note:
        結果はoutput/resultsディレクトリから読み込まれます。
        プロット画像のパスも含まれます。
    """
    results = {}
    
    result_dir = Path(OUTPUT_PATH) / "results"
    
    for ticker in tickers:
        if "." in ticker:
            file_ticker = ticker.split(".")[0]
        else:
            file_ticker = ticker
            
        result_path = result_dir / f"{file_ticker}_result.json"
        
        if result_path.exists():
            # 結果ファイル読み込み
            with open(result_path, "r", encoding="utf-8") as f:
                result = json.load(f)
                
            results[ticker] = {"stats": result}
            
            # プロットパスの追加
            plot_dir = Path(OUTPUT_PATH) / "plots"
            backtest_plot = plot_dir / f"{file_ticker}_backtest.png"
            stats_plot = plot_dir / f"{file_ticker}_stats.png"
            
            if backtest_plot.exists() and stats_plot.exists():
                results[ticker]["plots"] = {
                    "backtest": f"/plots/{file_ticker}_backtest.png",
                    "stats": f"/plots/{file_ticker}_stats.png",
                }
        else:
            results[ticker] = {"error": "結果が見つかりません"}
    
    return results

# 単一銘柄バックテスト
@app.post("/backtest/ticker/{ticker}", response_model=BacktestResult)
async def backtest_ticker(
    ticker: str,
    full_load: bool = FullLoad.PARTIAL.value,
    test_size: float = TestSize.TS_20.value,
    cash: float = Cash.C1M.value,
    commission: float = Commission.NORMAL.value,
    wait_for_completion: bool = WaitForCompletion.CONTINUE.value,
):
    """
    単一銘柄のバックテスト実行エンドポイント
    
    指定された銘柄のバックテストを実行します。
    パラメータとして銘柄コードと各種設定値を受け取り、
    Prefectフローを非同期に実行します。
    
    Args:
        ticker: 銘柄コード
        full_load: 全期間取得フラグ
        test_size: テストデータの割合
        cash: 初期資金
        commission: 取引手数料
        wait_for_completion: 完了を待つかどうか
        
    Returns:
        BacktestResult: バックテスト実行状態と結果
        
    Note:
        wait_for_completionがTrueの場合、処理が完了するまで待機します（最大300秒）。
        Falseの場合は即座にフロー実行IDを返し、クライアントは別途状態を確認する必要があります。
    """
    parameters = {
        "ticker": ticker,
        "full_load": full_load,
        "test_size": test_size,
        "cash": cash,
        "commission": commission,
    }
    
    try:
        # Prefectフロー実行リクエスト送信
        flow_run_id = await run_prefect_flow(
            flow_name="単一銘柄バックテストフロー",
            parameters=parameters,
            flow_run_name=f"Backtest-{ticker}-{int(time.time())}",
        )
        
        if wait_for_completion:
            # 完了を待つ場合、定期的に状態を確認
            max_wait_time = 300  # 最大待機時間（秒）
            check_interval = 5  # 確認間隔（秒）
            
            elapsed_time = 0
            while elapsed_time < max_wait_time:
                await asyncio.sleep(check_interval)
                elapsed_time += check_interval
                
                # 状態確認
                status = await get_flow_run_status(flow_run_id)
                
                if status["state"]["type"] in ["COMPLETED", "FAILED", "CANCELLED"]:
                    # 処理が完了または失敗した場合
                    if status["state"]["type"] == "COMPLETED":
                        # 結果取得
                        results = get_backtest_results([ticker])
                        return BacktestResult(
                            flow_run_id=flow_run_id,
                            status=status["state"]["type"],
                            results=results,
                        )
                    else:
                        # エラーの場合
                        return BacktestResult(
                            flow_run_id=flow_run_id,
                            status=status["state"]["type"],
                            error=f"フロー実行失敗: {status['state'].get('message', '不明なエラー')}",
                        )
            
            # タイムアウト
            return BacktestResult(
                flow_run_id=flow_run_id,
                status="TIMEOUT",
                error="処理がタイムアウトしました",
            )
        else:
            # 完了を待たない場合
            return BacktestResult(
                flow_run_id=flow_run_id,
                status="PENDING",
            )
    
    except Exception as e:
        return BacktestResult(
            status="ERROR",
            error=str(e),
        )

# 複数銘柄バックテスト
@app.post("/backtest/tickers", response_model=BacktestResult)
async def backtest_tickers(request: BacktestRequest):
    """
    複数銘柄のバックテスト実行エンドポイント
    
    複数の銘柄に対してバックテストを一括実行します。
    リクエストボディにJSON形式で銘柄リストと各種パラメータを指定します。
    
    Args:
        request: バックテストリクエスト（BacktestRequestモデル）
        
    Returns:
        BacktestResult: バックテスト実行状態と結果
        
    Note:
        このエンドポイントは常に非同期実行となり、即座にフロー実行IDを返します。
        クライアントは別途状態確認エンドポイントを使用して結果を取得する必要があります。
    """
    parameters = {
        "tickers": request.tickers,
        "full_load": request.full_load,
        "test_size": request.test_size,
        "cash": request.cash,
        "commission": request.commission,
    }
    
    try:
        # Prefectフロー実行リクエスト送信
        flow_run_id = await run_prefect_flow(
            flow_name="複数銘柄バックテストフロー",
            parameters=parameters,
            flow_run_name=f"MultiBacktest-{len(request.tickers)}-{int(time.time())}",
        )
        
        return BacktestResult(
            flow_run_id=flow_run_id,
            status="PENDING",
        )
    
    except Exception as e:
        return BacktestResult(
            status="ERROR",
            error=str(e),
        )

# 市場バックテスト
@app.post("/backtest/market", response_model=BacktestResult)
async def backtest_market(request: MarketBacktestRequest):
    """
    市場区分バックテスト実行エンドポイント
    
    指定された市場区分の全銘柄に対してバックテストを実行します。
    リクエストボディにJSON形式で市場区分と各種パラメータを指定します。
    
    Args:
        request: 市場バックテストリクエスト（MarketBacktestRequestモデル）
        
    Returns:
        BacktestResult: バックテスト実行状態と結果
        
    Note:
        このエンドポイントは常に非同期実行となり、即座にフロー実行IDを返します。
        クライアントは別途状態確認エンドポイントを使用して結果を取得する必要があります。
    """
    parameters = {
        "market": request.market,
        "full_load": request.full_load,
        "test_size": request.test_size,
        "cash": request.cash,
        "commission": request.commission,
    }
    
    try:
        # Prefectフロー実行リクエスト送信
        flow_run_id = await run_prefect_flow(
            flow_name="市場ベースバックテストフロー",
            parameters=parameters,
            flow_run_name=f"MarketBacktest-{request.market}-{int(time.time())}",
        )
        
        return BacktestResult(
            flow_run_id=flow_run_id,
            status="PENDING",
        )
    
    except Exception as e:
        return BacktestResult(
            status="ERROR",
            error=str(e),
        )

# フロー実行状態確認
@app.get("/flow-runs/{flow_run_id}", response_model=BacktestResult)
async def get_flow_run(flow_run_id: str):
    """
    フロー実行状態確認エンドポイント
    
    指定されたフロー実行IDの状態を確認し、完了していれば結果を返します。
    
    Args:
        flow_run_id: フロー実行ID
        
    Returns:
        BacktestResult: バックテスト実行状態と結果
        
    Note:
        フローが完了している場合は結果データも含めて返します。
        実行中や待機中の場合はステータスのみを返します。
    """
    try:
        # フロー実行状態取得
        status = await get_flow_run_status(flow_run_id)
        
        state_type = status["state"]["type"]
        
        if state_type == "COMPLETED":
            # 完了している場合は結果も取得
            
            # ティッカーの抽出（フロー名から推測）
            flow_name = status.get("name", "")
            if flow_name.startswith("Backtest-"):
                # 単一銘柄の場合
                ticker = flow_name.split("-")[1]
                results = get_backtest_results([ticker])
            else:
                # 複数銘柄や市場の場合は結果のみを返す
                # 注: 実際にはより複雑な結果取得ロジックが必要
                results = {"message": "複数銘柄または市場のバックテスト完了"}
            
            return BacktestResult(
                flow_run_id=flow_run_id,
                status=state_type,
                results=results,
            )
        elif state_type in ["FAILED", "CANCELLED"]:
            # エラーの場合
            return BacktestResult(
                flow_run_id=flow_run_id,
                status=state_type,
                error=status["state"].get("message", "不明なエラー"),
            )
        else:
            # 実行中や待機中の場合
            return BacktestResult(
                flow_run_id=flow_run_id,
                status=state_type,
            )
    
    except Exception as e:
        return BacktestResult(
            status="ERROR",
            error=str(e),
        )

# 上場情報・株式分割フロー実行
@app.post("/maintenance/listing-split", response_model=ListingSplitResult)
async def run_listing_split_flow(
    wait_for_completion: bool = WaitForCompletion.CONTINUE.value,
):
    """
    上場情報・株式分割フロー実行
    
    新規上場・上場廃止の情報を反映し、株式分割・株式合併の調整を行います。
    
    Args:
        wait_for_completion: 処理完了を待つか（True）、即時応答するか（False）
        
    Returns:
        フロー実行結果
    """
    try:
        # Prefectフロー実行リクエスト送信
        flow_run_id = await run_prefect_flow(
            flow_name="上場情報・株式分割フロー",
            parameters={},
            flow_run_name=f"ListingSplit-{int(time.time())}",
        )
        
        if wait_for_completion:
            # 完了を待つ場合、定期的に状態を確認
            max_wait_time = 300  # 最大待機時間（秒）
            check_interval = 5  # 確認間隔（秒）
            
            elapsed_time = 0
            while elapsed_time < max_wait_time:
                await asyncio.sleep(check_interval)
                elapsed_time += check_interval
                
                # 状態確認
                status = await get_flow_run_status(flow_run_id)
                
                if status["state"]["type"] in ["COMPLETED", "FAILED", "CANCELLED"]:
                    # 処理が完了または失敗した場合
                    if status["state"]["type"] == "COMPLETED":
                        return ListingSplitResult(
                            flow_run_id=flow_run_id,
                            status=status["state"]["type"],
                            results=status["state"].get("data", {}),
                        )
                    else:
                        # エラーの場合
                        return ListingSplitResult(
                            flow_run_id=flow_run_id,
                            status=status["state"]["type"],
                            error=f"フロー実行失敗: {status['state'].get('message', '不明なエラー')}",
                        )
            
            # タイムアウト
            return ListingSplitResult(
                flow_run_id=flow_run_id,
                status="TIMEOUT",
                error="処理がタイムアウトしました",
            )
        
        # 即時応答
        return ListingSplitResult(
            flow_run_id=flow_run_id,
            status="PENDING",
        )
    
    except Exception as e:
        return ListingSplitResult(
            flow_run_id="",
            status="ERROR",
            error=str(e),
        )

# 実行ファイルが直接実行された場合の処理
if __name__ == "__main__":
    
    # 静的ファイル配信の設定
    try:
        # プロットディレクトリをマウント
        plots_dir = Path(OUTPUT_PATH) / "plots"
        if plots_dir.exists():
            app.mount("/plots", StaticFiles(directory=str(plots_dir)), name="plots")
    except Exception as e:
        logger.error(f"静的ファイル設定エラー: {e}")
    
    # サーバー起動
    uvicorn.run(app, host="0.0.0.0", port=8000) 
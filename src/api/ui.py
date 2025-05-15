"""
Streamlit UI for バックテストシステム

このモジュールは、機械学習を用いた株式取引戦略のバックテストシステムの
Webユーザーインターフェースを提供します。Streamlitを使用して
パラメータ設定や結果表示のUIを構築しています。

主な機能:
- バックテストのパラメータ設定フォーム
- API連携によるバックテスト実行
- リアルタイム実行状態監視
- バックテスト結果の可視化表示
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union
import asyncio

import httpx
import pandas as pd
import streamlit as st
from PIL import Image

from src.utils.log_config import logger

# APIエンドポイント
API_URL = os.environ.get("API_URL", "http://localhost:8000")

# キャッシュタイムアウト設定（秒）
CACHE_TTL = 5

# タイトル設定
st.set_page_config(
    page_title="株式取引戦略バックテスト",
    page_icon="📈",
    layout="wide",
)

# CSS
st.markdown("""
<style>
    .main-title {
        text-align: center;
        font-size: 2.5rem;
        margin-bottom: 1rem;
    }
    .section-title {
        font-size: 1.5rem;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
    }
    .info-box {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .success-text {
        color: #0f5132;
    }
    .error-text {
        color: #842029;
    }
    .neutral-text {
        color: #055160;
    }
</style>
""", unsafe_allow_html=True)

# APIリクエスト処理
async def api_request(endpoint: str, method: str = "GET", params: dict = None, json_data: dict = None):
    """
    APIリクエスト処理
    
    FastAPI バックエンドに非同期リクエストを送信し、レスポンスを取得します。
    
    Args:
        endpoint: APIエンドポイントパス
        method: HTTPメソッド（"GET"または"POST"）
        params: クエリパラメータ
        json_data: POSTリクエスト用のJSONデータ
        
    Returns:
        レスポンスデータ（JSONをパースした辞書）またはNone（エラー時）
        
    Note:
        httpxライブラリを使用して非同期HTTPリクエストを実行します。
        エラー発生時はStreamlitのUI上にエラーメッセージを表示します。
    """
    url = f"{API_URL}{endpoint}"
    
    async with httpx.AsyncClient() as client:
        if method == "GET":
            response = await client.get(url, params=params)
        elif method == "POST":
            response = await client.post(url, params=params, json=json_data)
        else:
            st.error(f"サポートされていないHTTPメソッド: {method}")
            return None
    
    if response.status_code >= 400:
        st.error(f"APIエラー ({response.status_code}): {response.text}")
        return None
    
    return response.json()

# フロー実行状態確認
async def check_flow_run_status(flow_run_id: str):
    """
    フロー実行状態確認
    
    Prefectフロー実行IDを指定して、現在の実行状態を取得します。
    
    Args:
        flow_run_id: フロー実行ID
        
    Returns:
        フロー実行状態のレスポンス（辞書）またはNone（エラー時）
        
    Note:
        APIの"/flow-runs/{flow_run_id}"エンドポイントにリクエストを送信し、
        現在の状態や結果を取得します。
    """
    endpoint = f"/flow-runs/{flow_run_id}"
    return await api_request(endpoint)

# ダミー銘柄リスト
def get_dummy_tickers():
    """
    ダミー銘柄リスト取得
    
    テスト用のダミー銘柄リストを生成します。
    実際のプロジェクトでは、APIから取得するか、ローカルファイルから読み込みます。
    
    Returns:
        銘柄リスト（コード、銘柄名、市場区分のDataFrame）
        
    Note:
        このデモ実装では日本の主要企業5社のみをリストに含めています。
        実際の実装では、東証の上場企業リストなどを使用します。
    """
    dummy_data = {
        "コード": ["7203", "9984", "6758", "6861", "4755"],
        "銘柄名": ["トヨタ自動車", "ソフトバンクグループ", "ソニーグループ", "キーエンス", "楽天グループ"],
        "市場": ["プライム", "プライム", "プライム", "プライム", "プライム"]
    }
    
    return pd.DataFrame(dummy_data)

# メインアプリケーション
def main():
    """
    メインアプリケーション
    
    Streamlit UIのメイン処理です。サイドバーでのパラメータ設定、
    バックテスト実行、結果表示などの全体処理を制御します。
    
    処理の流れ:
    1. UIレイアウトとパラメータ入力フォームの表示
    2. 実行ボタンクリック時のAPIリクエスト送信
    3. 実行状態の定期的な確認
    4. 完了時の結果表示
    """
    
    # タイトル
    st.markdown('<div class="main-title">株式取引戦略のバックテスト</div>', unsafe_allow_html=True)
    
    # サイドバー：入力パラメータ
    with st.sidebar:
        st.markdown('<div class="section-title">パラメータ設定</div>', unsafe_allow_html=True)
        
        # バックテスト方式選択
        backtest_type = st.radio(
            "バックテスト方式",
            options=["単一銘柄", "複数銘柄", "市場区分"],
            index=0,
        )
        
        # 銘柄選択
        if backtest_type in ["単一銘柄", "複数銘柄"]:
            # ダミー銘柄リスト取得
            tickers_df = get_dummy_tickers()
            
            if backtest_type == "単一銘柄":
                # 単一銘柄選択
                ticker_options = [f"{row['コード']} ({row['銘柄名']})" for _, row in tickers_df.iterrows()]
                selected_ticker = st.selectbox("銘柄", options=ticker_options)
                
                # コード部分のみ抽出
                selected_code = selected_ticker.split(" ")[0]
                
            else:
                # 複数銘柄選択
                ticker_options = [f"{row['コード']} ({row['銘柄名']})" for _, row in tickers_df.iterrows()]
                selected_tickers = st.multiselect("銘柄（複数選択可）", options=ticker_options)
                
                # コード部分のみ抽出
                selected_codes = [ticker.split(" ")[0] for ticker in selected_tickers]
        
        # 市場区分選択
        if backtest_type == "市場区分":
            market = st.selectbox(
                "市場区分",
                options=["ALL", "プライム", "スタンダード", "グロース"],
                index=0,
            )
        
        # 共通パラメータ
        st.divider()
        st.markdown("**詳細設定**")
        
        # 全期間取得フラグ
        full_load = st.checkbox("全期間データ取得", value=False, help="チェックすると最新のデータを全期間取得します")
        
        # テストデータの割合
        test_size = st.slider("テストデータの割合", min_value=0.1, max_value=0.5, value=0.3, step=0.05, help="訓練データとテストデータの分割比率")
        
        # 初期資金
        cash = st.number_input("初期資金", min_value=1000, max_value=1000000, value=10000, step=1000, help="バックテスト開始時の資金")
        
        # 取引手数料
        commission = st.number_input("取引手数料", min_value=0.0, max_value=0.02, value=0.001, step=0.001, format="%.3f", help="取引あたりの手数料率")
        
        # 実行ボタン
        submit_button = st.button("バックテスト実行")
        
    # メイン画面
    # セッション状態の初期化
    if "flow_run_id" not in st.session_state:
        st.session_state.flow_run_id = None
    
    if "flow_status" not in st.session_state:
        st.session_state.flow_status = None
    
    if "results" not in st.session_state:
        st.session_state.results = None
    
    if "last_check_time" not in st.session_state:
        st.session_state.last_check_time = 0
    
    # バックテスト実行
    if submit_button:
        st.info("バックテストを開始します...")
        
        try:
            # APIエンドポイントとパラメータの設定
            if backtest_type == "単一銘柄":
                endpoint = f"/backtest/ticker/{selected_code}"
                params = {
                    "full_load": full_load,
                    "test_size": test_size,
                    "cash": cash,
                    "commission": commission,
                }
                json_data = None
                
            elif backtest_type == "複数銘柄":
                endpoint = "/backtest/tickers"
                params = {}
                json_data = {
                    "tickers": selected_codes,
                    "full_load": full_load,
                    "test_size": test_size,
                    "cash": cash,
                    "commission": commission,
                }
                
            else:  # 市場区分
                endpoint = "/backtest/market"
                params = {}
                json_data = {
                    "market": market,
                    "full_load": full_load,
                    "test_size": test_size,
                    "cash": cash,
                    "commission": commission,
                }
            
            # APIリクエスト
            response = asyncio.run(api_request(endpoint, method="POST", params=params, json_data=json_data))
            
            if response:
                # フロー実行ID保存
                st.session_state.flow_run_id = response.get("flow_run_id")
                st.session_state.flow_status = response.get("status")
                st.session_state.results = response.get("results")
                st.session_state.last_check_time = time.time()
                
                st.success(f"バックテスト開始: {st.session_state.flow_run_id}")
            else:
                st.error("APIリクエスト失敗")
                
        except Exception as e:
            st.error(f"バックテスト実行エラー: {e}")
    
    # フロー実行状態チェック
    if st.session_state.flow_run_id:
        # 最終チェックから一定時間経過している場合、状態を再取得
        current_time = time.time()
        
        if current_time - st.session_state.last_check_time >= CACHE_TTL:
            try:
                response = asyncio.run(check_flow_run_status(st.session_state.flow_run_id))
                
                if response:
                    # 状態更新
                    st.session_state.flow_status = response.get("status")
                    
                    # 完了していれば結果を取得
                    if response.get("status") == "COMPLETED":
                        st.session_state.results = response.get("results")
                    elif response.get("status") in ["FAILED", "CANCELLED"]:
                        st.error(f"バックテスト失敗: {response.get('error', '不明なエラー')}")
                    
                    # 時刻更新
                    st.session_state.last_check_time = current_time
            
            except Exception as e:
                st.error(f"状態確認エラー: {e}")
        
        # 現在の状態表示
        status_text = st.session_state.flow_status
        
        st.markdown(f"""
        <div class="info-box">
            <p><strong>フロー実行ID:</strong> {st.session_state.flow_run_id}</p>
            <p><strong>ステータス:</strong> <span class="{'success-text' if status_text == 'COMPLETED' else 'error-text' if status_text in ['FAILED', 'CANCELLED', 'ERROR'] else 'neutral-text'}">{status_text}</span></p>
        </div>
        """, unsafe_allow_html=True)
        
        # 完了していれば結果表示
        if st.session_state.flow_status == "COMPLETED" and st.session_state.results:
            st.markdown('<div class="section-title">バックテスト結果</div>', unsafe_allow_html=True)
            
            results = st.session_state.results
            
            # 単一銘柄の場合
            if backtest_type == "単一銘柄" and selected_code in results:
                ticker_result = results[selected_code]
                
                if "stats" in ticker_result:
                    stats = ticker_result["stats"]
                    
                    # 主要指標をカード形式で表示
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("リターン", f"{stats.get('Return', 0):.2f}%")
                    with col2:
                        st.metric("最大ドローダウン", f"{stats.get('Max Drawdown', 0):.2f}%")
                    with col3:
                        st.metric("勝率", f"{stats.get('Win Rate', 0):.2f}%")
                    with col4:
                        st.metric("シャープレシオ", f"{stats.get('Sharpe Ratio', 0):.2f}")
                    
                    # 統計情報の詳細をテーブルで表示
                    st.markdown("### 詳細統計")
                    stats_df = pd.DataFrame({
                        "指標": list(stats.keys()),
                        "値": list(stats.values())
                    })
                    st.dataframe(stats_df)
                
                # プロット表示
                if "plots" in ticker_result:
                    plots = ticker_result["plots"]
                    
                    st.markdown("### バックテストチャート")
                    
                    # バックテストチャート
                    if "backtest" in plots:
                        backtest_plot_url = f"{API_URL}{plots['backtest']}"
                        st.image(backtest_plot_url)
                    
                    # 統計チャート
                    if "stats" in plots:
                        stats_plot_url = f"{API_URL}{plots['stats']}"
                        st.image(stats_plot_url)
                
                # エラーがある場合
                if "error" in ticker_result:
                    st.error(f"エラー: {ticker_result['error']}")
            
            # 複数銘柄の場合
            elif backtest_type == "複数銘柄":
                # 実装内容: 複数銘柄の結果表示ロジック
                st.info("複数銘柄のバックテスト結果は集計表形式で表示されます")
                
                # 結果をデータフレームに変換
                result_rows = []
                
                for ticker, ticker_result in results.items():
                    if "stats" in ticker_result:
                        stats = ticker_result["stats"]
                        result_rows.append({
                            "銘柄コード": ticker,
                            "リターン(%)": stats.get("Return", 0),
                            "最大ドローダウン(%)": stats.get("Max Drawdown", 0),
                            "取引回数": stats.get("# Trades", 0),
                            "勝率(%)": stats.get("Win Rate", 0),
                            "シャープレシオ": stats.get("Sharpe Ratio", 0),
                        })
                
                if result_rows:
                    results_df = pd.DataFrame(result_rows)
                    st.dataframe(results_df)
                else:
                    st.warning("表示可能な結果がありません")
            
            # 市場区分の場合
            elif backtest_type == "市場区分":
                st.info(f"{market}市場のバックテスト結果集計")
                # 実装内容: 市場区分の結果表示ロジック（一例として対象銘柄数表示など）
                st.write(f"対象銘柄数: {len(results)}")

    # メンテナンスセクション
    st.header("📈 マーケットデータメンテナンス")
    st.markdown("""
    このセクションでは、株価データを最新の状態に保つための各種メンテナンス機能を提供します。
    上場状況の変更（新規上場・上場廃止）や株式分割・株式合併の反映が行えます。
    """)

    maint_col1, maint_col2 = st.columns(2)

    with maint_col1:
        if st.button("上場情報・株式分割調整を実行", type="primary"):
            with st.spinner("上場情報と株式分割の調整を実行中..."):
                # APIリクエスト
                response = asyncio.run(api_request("/maintenance/listing-split", method="POST", json_data={"wait_for_completion": True}))
                
                if response:
                    if response["status"] == "COMPLETED":
                        st.success("上場情報・株式分割調整が完了しました")
                        st.json(response["results"])
                    else:
                        st.error(f"処理エラー: {response.get('error', '不明なエラー')}")
                else:
                    st.error("APIリクエスト失敗")

    with maint_col2:
        st.info("""
        **このボタンの機能:**
        - 東証の新規上場・上場廃止情報を取得
        - 当日に新規上場した銘柄を追加
        - 当日に上場廃止した銘柄を削除
        - 当日の株式分割・株式合併情報を取得して株価を調整
        """)

    st.divider()

# アプリケーション実行
if __name__ == "__main__":
    main() 
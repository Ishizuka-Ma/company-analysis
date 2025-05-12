# 機械学習を用いた株式取引戦略のバックテストシステム

株価の過去データに基づいて機械学習モデルを訓練し、取引戦略を立案・バックテストするシステムです。

## 機能概要

- 日本株のデータ（日次OHLCV+Adj Close+Volume）を取得
- 特徴量エンジニアリングによる予測用データセット作成
- 機械学習モデル（RandomForest）による翌日株価上昇確率の予測
- バックテストによる戦略の評価
- Web UIからのバックテスト実行と結果可視化

## システム構成

- **フロント**: Streamlit
- **バックエンド**: FastAPI
- **ワークフロー管理**: Prefect 2
- **コンテナ化**: Docker / Docker Compose
- **データ取得**: yfinance
- **データ処理**: pandas / polars
- **機械学習**: scikit-learn
- **バックテスト**: backtesting.py
- **データストレージ**: Parquet / DuckDB

## 起動方法

### 開発環境での起動

1. 環境のセットアップ
   ```bash
   # Python仮想環境作成と依存関係インストール
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. Prefectサーバー起動
   ```bash
   prefect server start
   ```

3. 別ターミナルでPrefectエージェント起動
   ```bash
   prefect agent start -q default
   ```

4. APIサーバー起動
   ```bash
   uvicorn src.api.main:app --reload
   ```

5. Streamlit UI起動
   ```bash
   streamlit run src/api/ui.py
   ```

### Docker Composeでの起動

すべてのサービスを一括で起動：

```bash
docker-compose up
```

## ディレクトリ構造

```
project/
├── pyproject.toml          # 依存関係管理
├── requirements.txt        # 依存関係リスト
├── Dockerfile              # Dockerイメージ定義
├── compose.yaml            # Docker Compose定義
├── src/
│   ├── __init__.py
│   ├── flows/              # Prefectフロー定義
│   │   ├── __init__.py
│   │   └── stock_flow.py   # 株価取得・バックテストフロー
│   ├── tasks/              # 個別タスク
│   │   ├── __init__.py
│   │   ├── data_fetcher.py # データ取得
│   │   ├── data_processor.py # データ処理
│   │   └── backtest.py     # バックテスト実行
│   └── api/                # FastAPI App
│       ├── __init__.py
│       ├── main.py         # FastAPI定義
│       └── ui.py           # Streamlit UI
├── data/
│   ├── raw/                # 生データ
│   │   └── prices/         # 株価データ
│   ├── processed/          # 処理済みデータ
│   │   └── prices/
│   ├── feature/            # 特徴量データ
│   │   └── features/
│   └── meta/
│       └── latest_date.json # メタデータ
└── output/                 # 出力結果
    ├── plots/              # グラフ
    └── results/            # 結果JSON
```

## 使用方法

1. Streamlit UIにアクセス (http://localhost:8501)
2. バックテスト方式（単一銘柄、複数銘柄、市場区分）を選択
3. 銘柄や各種パラメータを設定
4. 「バックテスト実行」ボタンをクリック
5. 実行状態を確認しながら結果を待つ
6. 結果のチャートや統計情報を確認

## 開発者向け情報

- APIドキュメント: http://localhost:8000/docs
- Prefect UI: http://localhost:4200 
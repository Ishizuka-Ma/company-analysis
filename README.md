# 機械学習を用いた株式取引戦略のバックテストシステム

株価の過去データに基づいて機械学習モデルを訓練し、取引戦略を立案・バックテストするシステムです。

## 機能概要

- 日本株のデータ（日次OHLCV+Adj Close）を取得
- 特徴量エンジニアリングによる予測用データセット作成
- 機械学習モデル（RandomForest）による翌日株価上昇確率の予測
- バックテストによる戦略の評価
- Web UIからのバックテスト実行と結果可視化
- GitHub Actionsによる日次データ自動更新
- 新規上場・上場廃止情報の自動更新
- 株式分割・株式合併による株価データの自動調整

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
- **コード品質管理**: Ruff (Linter + Formatter)
- **パッケージ管理**: uv
- **CI/CD**: GitHub Actions

## 起動方法

### 開発環境での起動

1. 環境のセットアップ
   ```bash
   # uvのインストール (初回のみ)
   pip install uv
   
   # Python仮想環境作成と依存関係インストール
   uv venv
   
   # Windowsの場合
   .venv\Scripts\activate
   # macOS/Linuxの場合
   source .venv/bin/activate
   
   # 依存関係のインストール
   uv pip install -r requirements.txt
   
   # 開発用依存関係のインストール
   uv pip install -e ".[dev]"
   ```

2. データディレクトリのセットアップ
   ```bash
   # 必要なデータディレクトリを作成
   mkdir -p data/raw/prices
   mkdir -p data/processed/prices
   mkdir -p data/processed/listing
   mkdir -p data/feature
   mkdir -p data/meta
   ```

3. コード品質管理
   ```bash
   # コードフォーマット
   ruff format .
   
   # リンター実行
   ruff check .
   
   # 自動修正可能な問題を修正
   ruff check --fix .
   ```

4. Prefectサーバー起動
   ```bash
   prefect server start
   ```

5. 別ターミナルでPrefectエージェント起動
   ```bash
   # 仮想環境の有効化
   # Windowsの場合
   .venv\Scripts\activate
   # macOS/Linuxの場合
   source .venv/bin/activate
   
   prefect agent start -q default
   ```

6. 日次データ更新フローの実行
   ```bash
   # 仮想環境の有効化（まだ有効化していない場合）
   python -m src.flows.daily_update_flow
   ```

7. APIサーバー起動
   ```bash
   # 仮想環境の有効化（まだ有効化していない場合）
   uvicorn src.api.main:app --reload
   ```

8. Streamlit UI起動
   ```bash
   # 仮想環境の有効化（まだ有効化していない場合）
   streamlit run src/api/ui.py
   ```

### Docker Composeでの起動

すべてのサービスを一括で起動：

```bash
docker-compose up
```

日次データ更新フローの実行（Dockerコンテナ内）：

```bash
docker-compose exec app python -m src.flows.daily_update_flow
```

## ディレクトリ構造

```
project/
├── pyproject.toml          # 依存関係管理、Ruff設定
├── requirements.txt        # 依存関係リスト
├── Dockerfile              # Dockerイメージ定義
├── compose.yaml            # Docker Compose定義
├── .github/                # GitHub Actions設定
│   └── workflows/
│       └── daily_stock_update.yml  # 日次更新ワークフロー
├── src/
│   ├── __init__.py
│   ├── flows/              # Prefectフロー定義
│   │   ├── __init__.py
│   │   ├── stock_flow.py   # バックテストフロー
│   │   └── daily_update_flow.py  # 日次データ更新フロー
│   ├── tasks/              # 個別タスク
│   │   ├── __init__.py
│   │   ├── data_fetcher.py   # データ取得
│   │   ├── data_processor.py # データ処理
│   │   ├── backtest.py       # バックテスト実行
│   │   ├── listing_checker.py # 上場情報チェック
│   │   └── split_adjustment.py # 株式分割調整
│   ├── utils/              # ユーティリティ
│   │   ├── __init__.py
│   │   └── log_config.py   # ログ設定
│   └── api/                # FastAPI App
│       ├── __init__.py
│       ├── main.py         # FastAPI定義
│       └── ui.py           # Streamlit UI
├── data/
│   ├── raw/                # 生データ
│   │   └── prices/         # 株価データ
│   ├── processed/          # 処理済みデータ
│   │   ├── prices/         # 処理済み株価データ
│   │   └── listing/        # 上場情報データ
│   ├── feature/            # 特徴量データ
│   │   └── features/
│   └── meta/
│       └── latest_date.json # メタデータ
└── output/                 # 出力結果
    ├── plots/              # グラフ
    └── results/            # 結果JSON
```

## 使用方法

### バックテストの実行

1. Streamlit UIにアクセス (http://localhost:8501)
2. バックテスト方式（単一銘柄、複数銘柄、市場区分）を選択
3. 銘柄や各種パラメータを設定
4. 「バックテスト実行」ボタンをクリック
5. 実行状態を確認しながら結果を待つ
6. 結果のチャートや統計情報を確認

### 日次データ更新

日次データ更新フローは以下の処理を行います：

1. 新規上場と廃止に関する情報の更新（JPXウェブサイトからスクレイピング）
2. 株価データの取得（yfinanceから最新データを取得）
3. 株式分割と株式合併による過去の株価データの修正

このフローは以下の方法で実行できます：

- 手動実行: `python -m src.flows.daily_update_flow`
- GitHub Actions: リポジトリにpushすると毎日UTC 9:00（JST 18:00）に自動実行

注意: 以前は`stock_flow.py`内の`listing_split_flow()`関数でこれらの処理を行っていましたが、現在はすべての日次データ更新機能が`daily_update_flow.py`の`daily_update_flow()`関数に統合されています。

## 開発者向け情報

- APIドキュメント: http://localhost:8000/docs
- Prefect UI: http://localhost:4200 
- GitHub Actionsの手動実行: リポジトリの「Actions」タブから「日次株価データ更新」ワークフローを選択して「Run workflow」 
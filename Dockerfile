FROM python:3.9-slim

# 作業ディレクトリを/appに設定
WORKDIR /app

# 必要なパッケージをインストール
RUN pip install --no-cache-dir uv && \
    # シェルスクリプトの起動、実行高速化のためにBashをインストール
    apt-get update && \
    apt-get install -y --no-install-recommends bash && \
    # パッケージインストール後のクリーンアップ
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 依存関係をインストールする前にコピー
COPY requirements.txt pyproject.toml ./

# uvを使って依存関係をインストール
RUN uv pip install --no-cache-dir -r requirements.txt

# アプリケーションのコードをコピー
COPY . .

# コンテナ起動時のデフォルトコマンド
CMD ["python", "-m", "src.api.main"] 
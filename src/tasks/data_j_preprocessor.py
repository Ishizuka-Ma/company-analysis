# 東証上場銘柄一覧(https://www.jpx.co.jp/markets/statistics-equities/misc/01.html)のデータを加工

from pathlib import Path
import pandas as pd

KEEP_COLS = [
    "日付", "コード", "銘柄名", "市場・商品区分", "33業種区分", "17業種区分"
]
TARGET_MARKETS = {
    "プライム（内国株式）",
    "グロース（内国株式）",
    "スタンダード（内国株式）",
}
MARKET_SUFFIX = "（内国株式）"

def process_csv(in_file: str | Path,
                out_file: str | Path | None = None) -> pd.DataFrame:
    """
    東証上場銘柄の CSV を以下の条件で整形して DataFrame を返す。

    1. 列を「日付, コード, 銘柄名, 市場・商品区分, 33業種区分, 17業種区分」のみに絞る
    2. 「市場・商品区分」が
         - プライム（内国株式）
         - グロース（内国株式）
         - スタンダード（内国株式）
       の行だけを残す
    3. 「市場・商品区分」から文字列「（内国株式）」を取り除く
    4. 「日付」を datetime64[ns] 型へ変換する

    Parameters
    ----------
    in_file : str | Path
        入力 CSV のパス
    out_file : str | Path | None, default None
        保存先を指定すると UTF‑8‑SIG で書き出す。
        指定しない場合は書き出さず、戻り値だけ返す。

    Returns
    -------
    pd.DataFrame
        加工後の DataFrame
    """
    # --- 読み込み（BOM 付き CSV に合わせて utf-8-sig を指定） ---
    df = pd.read_csv(in_file, encoding="utf-8-sig", usecols=KEEP_COLS)

    df = df[df["市場・商品区分"].isin(TARGET_MARKETS)].copy()

    df["市場・商品区分"] = (
        df["市場・商品区分"].str.replace(MARKET_SUFFIX, "",  regex=False).str.strip()
    )
# , regex=False
    df["日付"] = pd.to_datetime(df["日付"].astype(str), format="%Y%m%d", errors="coerce")
    df.rename(columns={'日付': '更新日','市場・商品区分': '市場区分'}, inplace=True)
    df.reset_index(drop=True, inplace=True)

    if out_file is not None:
        df.to_csv(out_file, index=False, encoding="utf-8-sig")

    return df


# if __name__ == "__main__":
#     cleaned = process_csv("data_j.csv", "preprocess_data_j.csv")
#     print(cleaned)

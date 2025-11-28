# tracking_writer.py

import polars as pl
import os
from config import SUMMARY_FILE, DIFF_FILE

def append_summary(summary_dict):
    df = pl.DataFrame([summary_dict])

    # Nếu file chưa có → write normally
    if not os.path.exists(SUMMARY_FILE):
        df.write_csv(SUMMARY_FILE)
    else:
        # Append bằng text mode
        with open(SUMMARY_FILE, "a", encoding="utf-8") as f:
            # ghi nội dung CSV nhưng không ghi header
            f.write(df.write_csv().split("\n", 1)[1])  # bỏ dòng header


def append_diff_item(diff_df):
    if diff_df is None or diff_df.shape[0] == 0:
        return

    if not os.path.exists(DIFF_FILE):
        diff_df.write_csv(DIFF_FILE)
    else:
        with open(DIFF_FILE, "a", encoding="utf-8") as f:
            f.write(diff_df.write_csv().split("\n", 1)[1])  # bỏ header

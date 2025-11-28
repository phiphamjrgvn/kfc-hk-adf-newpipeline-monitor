import sys
import os
module_path = r"D:\DE\PHV-DATA-ENGINEER\src\regional\hk\sales\child_table\ctp_cti\kfc"
if module_path not in sys.path:
    sys.path.append(module_path)
from dc_pos_sales_item_kfc_landing import DCPosSalesItemLandingKFC
print(dir(DCPosSalesItemLandingKFC))
import sys, os
sys.path.append(r"D:\DE\PHV-DATA-ENGINEER")
from src.regional.hk.sales.etl_utils import ETLClient, DATABASE_CONNECTIONS
warehouse_client = ETLClient(db_config=DATABASE_CONNECTIONS[0])
import pickle
import os
from google.cloud import secretmanager
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window
from datetime import datetime
import polars as pl
import pyarrow as pa
import pytz
from src.regional.hk.sales.etl_utils import ETLClient, DATABASE_CONNECTIONS
vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
from pipelines.spark.scripts.hk.spark_ds_agg_f_unitsold import SparkDSUnitsold
print("PYTHONPATH:", sys.path)
import polars as pl
import pandas as pd
from datetime import datetime
from tracking_writer import append_summary, append_diff_item
from get_raw_data_checked import get_raw_data_checked
from config import BRAND

from src.regional.hk.sales.etl_utils import ETLClient, DATABASE_CONNECTIONS
from dc_pos_sales_item_kfc_landing import DCPosSalesItemLandingKFC
from get_raw_data_checked import get_dap_connection


def run_pipeline_one_day(date_skey: str):

    warehouse_client = ETLClient(db_config=DATABASE_CONNECTIONS[0])
    dc_instance = DCPosSalesItemLandingKFC(warehouse_client)

    print(f"\n=========== RUNNING DATE {date_skey} ===========")

    # Convert date format for CTI
    date_for_cti = datetime.strptime(date_skey, "%Y%m%d").strftime("%Y-%m-%d")

    # Step 1: Load raw data with mismatch detection
    (
        df_cti,
        df_d_loc,
        df_sales_channel,
        df_scd_item_table,
        df_currency,
        df_scd_item_table_2,
        df_l_day_part_weekday,
        df_d_hour_range,
        df_olo_order,
        df_item_table,
        df_item_table_2
    ) = get_raw_data_checked(dc_instance, BRAND, date_skey)

    shapes = {"date": date_skey, "df_cti": df_cti.shape[0]}

    # === FULL PIPELINE STEPS ===
    cti_lookup = dc_instance.base_transform_cti(df_cti, df_l_day_part_weekday)
    shapes["cti_lookup"] = cti_lookup.shape[0]

    df_cti_new_column = dc_instance.create_new_column_phase1(BRAND, cti_lookup)
    shapes["df_cti_new_column"] = df_cti_new_column.shape[0]

    df_cti_new_column_3 = dc_instance.create_new_column_phase2(df_cti_new_column)
    shapes["df_cti_new_column_3"] = df_cti_new_column_3.shape[0]

    df_cti_join_8 = dc_instance.join_table_cti(
        df_cti_new_column_3,
        df_d_loc,
        df_sales_channel,
        df_scd_item_table,
        df_scd_item_table_2,
        df_item_table,
        df_item_table_2,
        df_olo_order,
        df_currency
    )
    shapes["df_cti_join_8"] = df_cti_join_8.shape[0]

    df_cti_final_transform_4 = dc_instance.transform_cti_phase2(df_cti_join_8)
    shapes["df_cti_final_transform_4"] = df_cti_final_transform_4.shape[0]

    df_cti_final_transform_7 = dc_instance.mapping_cti(df_cti_final_transform_4)
    shapes["df_cti_final_transform_7"] = df_cti_final_transform_7.shape[0]

    df_no_duplicate, df_duplicate = dc_instance.separate_duplicates(df_cti_final_transform_7)
    shapes["df_no_duplicate"] = df_no_duplicate.shape[0]
    shapes["df_duplicate"] = df_duplicate.shape[0]

    transform_df = dc_instance.transform_duplicates(df_duplicate)
    shapes["transform_df"] = transform_df.shape[0]

    finalize_result = dc_instance.finalize_results(transform_df)
    shapes["finalize_result"] = finalize_result.shape[0]

    finalize_duplicate_branch = dc_instance.finalize_duplicate_branch(finalize_result)
    shapes["finalize_duplicate_branch"] = finalize_duplicate_branch.shape[0]

    merge_duplicate_and_normal = dc_instance.merge_duplicate_and_normal(df_no_duplicate, finalize_duplicate_branch)
    shapes["merge_duplicate_and_normal"] = merge_duplicate_and_normal.shape[0]

    apply_check_duplicate_seq = dc_instance.apply_check_duplicate_seq(merge_duplicate_and_normal, df_d_hour_range)
    shapes["apply_check_duplicate_seq"] = apply_check_duplicate_seq.shape[0]


    # Final standard formatting
    def transform_cti_only(df):
        final_cols = [
    'item_sales_bkey','scd_item_skey','brand','uniqu_ticket_number',
    'loc_skey','day_part_no','main_item_skey','detail_item_skey',
    'date_skey','vip_pool_skey','storl_vip_no','yuu_vip_no','vip_type',
    'org_sales_channel_code','sales_channel_code','sales_channel_desc',
    'loc_code','olo_order_mode_desc','store_name','store_chi_name',
    'deliver_info_skey','long','lag','food_type','main_item_code',
    'main_item_desc','org_detail_item_code','detail_item_code','detail_item_desc',
    'sales_method_group','sales_method_sub_group','sales_method_type',
    'kfc_main_item_plu_code','kfc_detail_item_plu_code',
    'orig_sales_amt','orig_sales_amt_hkd','unit_price','qty','disc_amt',
    'disc_amt_hkd','cost_hkd','net_sales_amt_hkd','gross_profit_amt_hkd',
    'main_item_sales_amt_hkd','main_item_cost_hkd','main_item_disc_amt_hkd',
    'main_item_net_sales_amt_hkd','main_item_gross_profit_amt_hkd','main_item_qty',
    'detail_item_orig_sales_amt','detail_item_sales_amt','detail_item_net_sales_amt_hkd',
    'detail_item_gross_profit_amt_hkd','detail_item_qty',
    'ticket_id','ticket_no','ticket_group','ticket_seq','ticket_sub_seq',
    'xref','dept','sub_code','level_1','desc1','desc2',
    'cashr','trans_date','trans_time','handl_id',
    'post','remark','k1','sync','seat',
    'is_sales','ytd_comp_store_flag','is_advance_order',
    'current_modify_date','current_modify_by','last_modify_date','last_modify_by',
    'create_date','create_by','is_groupbuy','d_code',
    'cti_points','cti_oprice','cti_rx','cti_tax1','cti_tax2','cti_tax3',
    'cti_t_able','cti_d_able','cti_sc_able','cti_tomain','cti_tax_group',
    'cti_tn','cti_fgroup','cti_calcost','cti_xxref1','cti_ixrewards',
    'cti_ixrewardrd','cti_irdrequire','cti_syncid','cti_old','cti_bumped',
    'cti_nsitem','cti_nmnp','cti_siflag','cti_siflag2',
    'cti_ddx','cti_pname1','cti_pname2','cti_mapcode','cti_pmcode',
    'cti_dx1','cti_dx2','cti_spf','cti_mxtt','cti_mxeo','cti_mxnew',
    'cti_txtn','cti_txhandleby','cti_txtime','cti_ooprice',
    'cti_odx1','cti_odx2','cti_ospf','cti_cbc',
    'kfc_non_food_type',
    'Is_Duplicate',     
    'ratio_amount','ratio_discount',
    'hour_range_skey'
]
  # (anh dùng lại list final schema của anh)
        for col in final_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        return df.select(final_cols)

    final_df = transform_cti_only(apply_check_duplicate_seq)
    # DEBUG: check one item to verify qty
    print("\n========== DEBUG CHECK QTY ==========")
    print(
        final_df.filter(pl.col("detail_item_code") == "AAG06X")
                .select(["detail_item_code", "qty"])
    )
    print("========== END DEBUG ==========\n")

    shapes["final_df"] = final_df.shape[0]


    # === Compare with DAP ===
    conn = get_dap_connection()
    query = f"""
        SELECT detail_item_code, SUM(qty) AS sum
        FROM dc_pos_sales_item
        WHERE brand = '{BRAND}' AND date_skey = {date_skey}
        GROUP BY detail_item_code
    """
    dap_df = pl.from_pandas(pd.read_sql(query, conn))

    new_groupby = final_df.group_by("detail_item_code").agg(
        pl.col("qty").cast(pl.Float64).sum().alias("sum_QTY")
    )
    # FULL OUTER JOIN — lấy ALL sản phẩm trong ngày
    diff_df = dap_df.join(
        new_groupby,
        left_on="detail_item_code",
        right_on="detail_item_code",
        how="outer"
    ).with_columns([
        pl.col("sum").cast(pl.Float64),
        pl.col("sum_QTY").cast(pl.Float64),
    ]).with_columns([
        (pl.col("sum_QTY").fill_null(0) - pl.col("sum").fill_null(0)).alias("diff"),
        pl.lit(date_skey).alias("date")
    ])

    # For summary only: count diff items
    diff_items_count = diff_df.filter(pl.col("diff") != 0).shape[0]

    shapes["dap_total_items"] = dap_df.shape[0]
    shapes["new_total_items"] = new_groupby.shape[0]
    shapes["diff_item_count"] = diff_items_count
    shapes["diff_percent"] = (diff_items_count / dap_df.shape[0]) * 100 if dap_df.shape[0] > 0 else 0

    # Save FULL table
    append_diff_item(diff_df)
    # Save result
    append_summary(shapes)


    # ======= SUMMARY OUTPUT TO TERMINAL =======
    diff_count = shapes["diff_item_count"]
    diff_percent = shapes["diff_percent"]

    print("\n---------------- DAILY VALIDATION RESULT ----------------")
    print(f"DATE: {date_skey}")
    print(f"CTI ROWS: {shapes['df_cti']}")
    print(f"FINAL PIPELINE ROWS: {shapes['final_df']}")
    print(f"DAP ITEMS: {shapes['dap_total_items']}")
    print(f"NEW PIPELINE ITEMS: {shapes['new_total_items']}")

    if diff_count == 0:
        print(f"DIFF STATUS: ✅ NO (0 mismatched items)")
    else:
        print(f"DIFF STATUS: ❌ YES ({diff_count} mismatched items)")

    print(f"DIFF PERCENT: {diff_percent:.2f}%")
    print("-----------------------------------------------------------\n")
    return shapes

# get_raw_data_checked.py

import polars as pl
import pandas as pd
import pyodbc


def get_dap_connection():
    server = 'jrghk-dap-datawarehouse-server-pro.database.windows.net'
    database = 'jrghk_dap_datawarehouse_pro01'
    username = 'dapadm'
    password = 'jrghkP@ssw0rd'
    driver = 'ODBC Driver 17 for SQL Server'

    conn_str = f"""
        DRIVER={{{driver}}};
        SERVER={server};
        DATABASE={database};
        UID={username};
        PWD={password};
    """
    return pyodbc.connect(conn_str)


def load_dap_tables(brand):
    conn = get_dap_connection()

    query_scd = f"""
    SELECT scd_item_skey, pos_fcode, brand, pos_item_desc
    FROM ds_scd_item
    WHERE brand = '{brand}' AND current_flag = 'Y'
    """
    query_item = f"""
    SELECT pos_fcode, brand, code_food_mlist, erp_item_code,
           pos_item_desc, pos_item_chi_desc, item_skey, food_type, non_food_type
    FROM ds_l_kfc_non_sales_item_view
    WHERE brand = '{brand}'
    """

    dap_scd = pl.from_pandas(pd.read_sql(query_scd, conn))
    dap_item = pl.from_pandas(pd.read_sql(query_item, conn))

    return dap_scd, dap_item


def check_and_override(bq_df, dap_df, table_name):
    """
    If row count mismatch → choose DAP.
    """
    if bq_df.shape[0] != dap_df.shape[0]:
        print(f"[WARN] MISMATCH {table_name}: BQ={bq_df.shape[0]}, DAP={dap_df.shape[0]} → using DAP")
        return dap_df
    else:
        print(f"[OK] {table_name} match: {bq_df.shape[0]}")
        return bq_df


def get_raw_data_checked(dc_instance, brand, missing_date):
    """
    Wrapper → gọi get_raw_data() → kiểm tra bảng → override bằng DAP nếu mismatch.
    """

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
    ) = dc_instance.get_raw_data(brand, missing_date)

    # Load DAP versions
    dap_scd, dap_item = load_dap_tables(brand)

    # Compare & override
    df_scd_item_table = check_and_override(df_scd_item_table, dap_scd, "ds_scd_item_table")
    df_scd_item_table_2 = df_scd_item_table.clone()

    df_item_table = check_and_override(df_item_table, dap_item, "item_table")
    df_item_table_2 = df_item_table.clone()

    return (
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
    )

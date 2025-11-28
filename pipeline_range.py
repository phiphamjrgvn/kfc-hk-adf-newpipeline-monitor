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
from datetime import datetime, timedelta
from pipeline_day import run_pipeline_one_day


def run_pipeline_range(from_date: str, to_date: str):

    d1 = datetime.strptime(from_date, "%Y%m%d")
    d2 = datetime.strptime(to_date, "%Y%m%d")

    while d1 <= d2:
        day = d1.strftime("%Y%m%d")
        try:
            run_pipeline_one_day(day)
        except Exception as e:
            print(f"[ERROR] Failed on {day}: {e}")
        d1 += timedelta(days=1)


if __name__ == "__main__":
    run_pipeline_range("20251101", "20251130")

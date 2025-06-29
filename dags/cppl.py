from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
import math

# DB and Elasticsearch settings
DB_URL = "mysql+pymysql://cabletica_dataeng_views:cabletica_dataeng@172.18.3.232:3306/cabletica?ssl=true&ssl_verify_cert=false"
ES_HOST = "https://192.168.102.20:9200"
ES_INDEX = "capacity_planning"
SQL_DIR = "/opt/airflow/dags/app/sql"
TMP_DIR = "/tmp"

QUERY_MAP = {
    "up_sg_util_facts": "nxt-query-up_sg_util_facts.sql",
    "up_util_facts": "nxt-query-up_util_facts.sql",
    "down_sg_util_facts": "nxt-query-down_sg_util_facts.sql",
    "cm_data_day_facts": "nxt-query-cm_data_day_facts.sql",
    "down_channel_config": "nxt-query-down_channel_config.sql",
    "up_channel_config": "nxt-query-up_channel_config.sql",
    "join_conf_phy": "nxt-query-jdbc_join_conf_phy.sql",
    "modem_to_channel": "nxt-query-cm_scq_up_day_facts.sql",
    "channel_to_fiber": "nxt-query-chan_to_phy_fiber_only.sql",
    "us_ds_cm_counts": "nxt-query-us-ds-cm-counts.sql"
}

os.makedirs(TMP_DIR, exist_ok=True)


def run_query(task_id, **kwargs):
    execution_date = kwargs["execution_date"]
    data_interval_start = kwargs["data_interval_start"]
    data_interval_end = kwargs["data_interval_end"]
    #start_date = (kwargs["data_interval_start"] - timedelta(days=7)).strftime("%Y-%m-%d")
    #end_date = (kwargs["data_interval_end"] - timedelta(days=7)).strftime("%Y-%m-%d")
    start_date = kwargs["data_interval_start"].strftime("%Y-%m-%d")
    end_date = kwargs["data_interval_end"].strftime("%Y-%m-%d")
    sql_path = os.path.join(SQL_DIR, QUERY_MAP[task_id])
    
    print(f"[{task_id}] Execution Date: {execution_date}")
    print(f"[{task_id}] Interval: {start_date} → {end_date}")
    print(f"[{task_id}] Interval: {data_interval_start} → {data_interval_end}")

    with open(sql_path) as f:
        raw_sql = f.read()

    query = raw_sql.replace("{{start_date}}", start_date).replace("{{end_date}}", end_date)
    engine = create_engine(DB_URL)
    df = pd.read_sql(query, engine)
    df.to_csv(f"{TMP_DIR}/{task_id}.csv", index=False)

def merge_utilization():
    up = pd.read_csv(f"{TMP_DIR}/up_sg_util_facts.csv")
    down = pd.read_csv(f"{TMP_DIR}/down_sg_util_facts.csv")
    df = up.merge(down, on=["CMTS_ID", "CABLE_MAC"], how="left", suffixes=("", "_ds"))
    df.to_csv(f"{TMP_DIR}/merge_base.csv", index=False)

def merge_cm_profiles():
    df = pd.read_csv(f"{TMP_DIR}/merge_base.csv")
    cm = pd.read_csv(f"{TMP_DIR}/cm_data_day_facts.csv")
    df = df.merge(cm, on=["CMTS_ID", "CABLE_MAC"], how="left")
    df.to_csv(f"{TMP_DIR}/merge_base.csv", index=False)

def merge_channel_config():
    df = pd.read_csv(f"{TMP_DIR}/merge_base.csv")
    down_chan = pd.read_csv(f"{TMP_DIR}/down_channel_config.csv")
    df = df.merge(down_chan, on=["CMTS_ID", "CABLE_MAC"], how="left")
    df.to_csv(f"{TMP_DIR}/merge_base.csv", index=False)

def merge_topology():
    df = pd.read_csv(f"{TMP_DIR}/merge_base.csv")
    topo = pd.read_csv(f"{TMP_DIR}/join_conf_phy.csv")

    # Merge topology data
    df = df.merge(
        topo[["CMTS_ID", "CABLE_MAC", "SG_ID", "FIBER_NODE"]],
        on=["CMTS_ID", "CABLE_MAC", "SG_ID"],
        how="left"
    )

    # SG_SIZE is now handled in add_node_cm_count from us_ds_cm_counts.csv
    df.to_csv(f"{TMP_DIR}/merge_base.csv", index=False)

def add_uchc():
    df = pd.read_csv(f"{TMP_DIR}/merge_base.csv")
    up_channels = pd.read_csv(f"{TMP_DIR}/up_channel_config.csv")

    uchc_df = (
        up_channels
        .groupby(["CMTS_ID", "CABLE_MAC", "SG_ID"])["CHANNEL_NAME"]
        .nunique()
        .reset_index(name="UCHC")
    )

    df = df.drop(columns=["UCHC"], errors="ignore")  # Remove bad UCHC if present
    df = df.merge(uchc_df, on=["CMTS_ID", "CABLE_MAC", "SG_ID"], how="left")
    df.to_csv(f"{TMP_DIR}/merge_base.csv", index=False)


def add_node_cm_count():
    df = pd.read_csv(f"{TMP_DIR}/merge_base.csv")
    cm_counts = pd.read_csv(f"{TMP_DIR}/us_ds_cm_counts.csv")

    # 🟡 Step 1: Merge SG-level fields
    df = df.merge(
        cm_counts[[
            "CMTS_ID", "CABLE_MAC", "SG_ID",
            "US_CM_COUNT", "SG_SIZE",
            "DOCSIS20_CM_COUNT", "DOCSIS30_CM_COUNT", "DOCSIS31_CM_COUNT"
        ]],
        on=["CMTS_ID", "CABLE_MAC", "SG_ID"],
        how="left"
    )

    # 🟢 Step 2: Calculate DS_CM_COUNT = sum of US_CM_COUNT for same (CMTS_ID, CABLE_MAC)
    ds_counts = (
        cm_counts
        .groupby(["CMTS_ID", "CABLE_MAC"], as_index=False)
        .agg(DS_CM_COUNT=("US_CM_COUNT", "sum"))
    )

    df = df.merge(
        ds_counts,
        on=["CMTS_ID", "CABLE_MAC"],
        how="left"
    )

    df.to_csv(f"{TMP_DIR}/merge_base.csv", index=False)

def add_ms_utilization():
    df = pd.read_csv(f"{TMP_DIR}/merge_base.csv")
    ms_df = pd.read_csv(f"{TMP_DIR}/up_util_facts.csv")
    up_channels = pd.read_csv(f"{TMP_DIR}/up_channel_config.csv")

    # First, get SG_ID for each row in ms_df
    ms_with_sg = ms_df.merge(
        up_channels[["CMTS_ID", "CABLE_MAC", "CHANNEL_NAME", "SG_ID"]],
        on=["CMTS_ID", "CABLE_MAC", "CHANNEL_NAME"],
        how="left"
    )

    # Then group by SG_ID level (some CHANNEL_NAMEs may belong to same SG)
    max_ms = (
        ms_with_sg
        .groupby(["CMTS_ID", "CABLE_MAC", "SG_ID"])["MS_UTILIZATION"]
        .max()
        .reset_index()
    )

    # Merge into the base dataframe
    df = df.merge(max_ms, on=["CMTS_ID", "CABLE_MAC", "SG_ID"], how="left")
    df.to_csv(f"{TMP_DIR}/merge_final.csv", index=False)

def push_to_elasticsearch(**kwargs):
    df = pd.read_csv(f"{TMP_DIR}/merge_final.csv")
    es = Elasticsearch(ES_HOST, basic_auth=("elastic", "Ben-fong-torres-3"), verify_certs=False)
    execution_date = kwargs['execution_date']
    week_start = execution_date - timedelta(days=execution_date.weekday() + 7)
    #run_ts = week_start.strftime("%Y-%m-%dT00:00:00Z")
    start_date = kwargs["data_interval_start"]
    next_day = start_date + timedelta(days=1)
    run_ts = next_day.strftime("%Y-%m-%dT00:00:00Z")
    print(f"this is the sent day:{run_ts}")
    
    # Replace NaNs with 0.0 for numeric fields
    df = df.fillna({
    "ms_utilization": 0.0,
    "us_mix_profile": 0.0,
    "ds_mix_profile": 0.0,
    "us_modem_count": 0,
    "ds_modem_count": 0,
    "sg_size": 0,
    "docsis20_cm_count": 0,
    "docsis30_cm_count": 0,
    "docsis31_cm_count": 0
    })


    actions = []
    for _, row in df.iterrows():
        try:
            if pd.isna(row["FIBER_NODE"]):
                continue

            doc = {
                "@timestamp": run_ts,
                "@version": "1",
                "type": "up-sg-util-facts",
                "hub_id": row["HUB_ID"],
                "cmts_id": row["CMTS_ID"],
                "cable_mac": row["CABLE_MAC"],
                "sg_id": int(row["SG_ID"]) if not pd.isna(row.get("SG_ID")) else None,
                "G-DSG": f"{row['CMTS_ID']} {row['CABLE_MAC']}",
                "G-USG": f"{row['CMTS_ID']} {row['CABLE_MAC']} {row['SG_ID']}",
                "iso_time_s": run_ts,
                "fiber_node": row["FIBER_NODE"],
                "ds_modem_count": int(row["DS_CM_COUNT"]) if not pd.isna(row.get("DS_CM_COUNT")) else None,
                "us_modem_count": int(row["US_CM_COUNT"]) if not pd.isna(row.get("US_CM_COUNT")) else None,
                "sg_size": int(row["SG_SIZE"]) if not pd.isna(row.get("SG_SIZE")) else None,
                "us_max_util": int(row["usMaxUtilization"]) if not pd.isna(row.get("usMaxUtilization")) else None,
                "us_avg_util": int(row["usAvgUtilization"]) if not pd.isna(row.get("usAvgUtilization")) else None,
                "us_above_thresh": int(row["usAboveThresholdCount"]) if not pd.isna(row.get("usAboveThresholdCount")) else None,
                "us_chcount": int(row["UCHC"]) if not pd.isna(row.get("UCHC")) else None,
                "ds_max_util": int(row["dsMaxUtilization"]) if not pd.isna(row.get("dsMaxUtilization")) else None,
                "ds_avg_util": int(row["dsAvgUtilization"]) if not pd.isna(row.get("dsAvgUtilization")) else None,
                "ds_above_thresh": int(row["dsAboveThresholdCount"]) if not pd.isna(row.get("dsAboveThresholdCount")) else None,
                "ds_chcount": int(row["CHANNEL_COUNT"]) if not pd.isna(row.get("CHANNEL_COUNT")) else None,
                "docsis20_cm_count": int(row["DOCSIS20_CM_COUNT"]) if not pd.isna(row.get("DOCSIS20_CM_COUNT")) else None,
                "docsis30_cm_count": int(row["DOCSIS30_CM_COUNT"]) if not pd.isna(row.get("DOCSIS30_CM_COUNT")) else None,
                "docsis31_cm_count": int(row["DOCSIS31_CM_COUNT"]) if not pd.isna(row.get("DOCSIS31_CM_COUNT")) else None,
                "ms_utilization": int(row["MS_UTILIZATION"]) if not pd.isna(row.get("MS_UTILIZATION")) else None,
                "ds_mix_profile": int(row["DS_AVG_PROFILE"]) if not pd.isna(row.get("DS_AVG_PROFILE")) else None,
                "us_mix_profile": int(row["US_AVG_PROFILE"]) if not pd.isna(row.get("US_AVG_PROFILE")) else None
            }

            actions.append({
                "_index": ES_INDEX,
                "_source": doc,
                "doc_as_upsert": True
            })

        except Exception as err:
            print(f"⚠️ Error processing row: {err}")

    if not actions:
        print("⚠️ No documents to push — actions list is empty.")
        return

    try:
        print(f"Prepared {len(actions)} documents to push out of {len(df)} rows")
        helpers.bulk(es, actions, chunk_size=500, request_timeout=60)
        print(f"✅ Pushed {len(actions)} docs to Elasticsearch index {ES_INDEX}")
    except BulkIndexError as e:
        print(f"❌ Bulk insert failed: {len(e.errors)} document(s) failed.")
        for error in e.errors:
            print("----")
            print(error)        

with DAG(
    dag_id="capacity_enrichment_dag",
    max_active_runs=1,  # Only 1 active DAG run at a time
    schedule_interval="0 6 * * 1",
    start_date=datetime(2025, 6, 8),
    end_date=datetime(2025, 11, 5),
    catchup=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["capacity", "elasticsearch"]
) as dag:

    query_tasks = []
    previous_task = None

    for task_id in QUERY_MAP:
        task = PythonOperator(
            task_id=f"run_{task_id}_query",
            python_callable=run_query,
            op_kwargs={"task_id": task_id},
            provide_context=True
        )
        query_tasks.append(task)
        if previous_task:
            previous_task >> task
        previous_task = task

    merge1 = PythonOperator(task_id="merge_utilization", python_callable=merge_utilization)
    merge2 = PythonOperator(task_id="merge_cm_profiles", python_callable=merge_cm_profiles)
    merge3 = PythonOperator(task_id="merge_channel_config", python_callable=merge_channel_config)
    merge4 = PythonOperator(task_id="merge_topology", python_callable=merge_topology)
    merge5 = PythonOperator(task_id="add_uchc", python_callable=add_uchc)
    merge6 = PythonOperator(task_id="add_node_cm_count", python_callable=add_node_cm_count)
    merge7 = PythonOperator(task_id="add_ms_utilization", python_callable=add_ms_utilization)
    push = PythonOperator(task_id="push_to_elasticsearch", python_callable=push_to_elasticsearch, provide_context=True)

    previous_task >> merge1 >> merge2 >> merge3 >> merge4 >> merge5 >> merge6 >> merge7 >> push

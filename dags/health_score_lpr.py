from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import os
from pendulum import timezone
# ========== Task Functions ==========


def log_airflow_dates(**context):
    execution_date = context.get("execution_date")
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    logical_date = context.get("logical_date")
    current_time = datetime.now()

    # This is the date you use to filter PTIME = (end - 1 day)
    ptime_date = (data_interval_end - timedelta(days=1)).date()

    print("🧠 --- Airflow DAG Execution Context ---")
    print(f"🕒 execution_date:      {execution_date} (Airflow 'logical' date)")
    print(f"📆 data_interval_start: {data_interval_start}")
    print(f"📆 data_interval_end:   {data_interval_end}")
    print(f"🔁 logical_date:        {logical_date}")
    print(f"⏱  current_time:        {current_time}")
    print(f"📌 PTIME (used in SQL): {ptime_date}")
    print("✅ -------------------------------------")

def load_data(**context):
    execution_date = context["execution_date"]
    mysql_config = {
        "host": "172.18.3.232",
        "user": "libertypr_dataeng_views",
        "password": "libertypr_dataeng",
        "database": "libertypr",
        "main_table": "CM_SCQ_UP_DAY_FACTS",
        "node_table": "CHANNEL_TO_CMTS_FIBER_NODE",
        "status_table": "CM_STATUS_DAY_FACTS"
    }

    db_url = (
        f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
        f"@{mysql_config['host']}:3306/{mysql_config['database']}?ssl=true&ssl_verify_cert=false"
    )
    engine = create_engine(db_url)
    target_date = context["data_interval_end"].date() - timedelta(days=1)
    print('target date')
    print(target_date)

    modem_query = f"""
    SELECT PTIME, PATH_2, CMTS_ID, CM_MAC, CHANNEL_NAME, CER_MAX, CCER_MAX, SNR_MIN, TX_MAX, PARTIAL_SERVICE
    FROM {mysql_config['main_table']}
    WHERE PTIME = '{target_date}'
    """
    node_map_query = f"SELECT * FROM {mysql_config['node_table']}"
    status_query = f"SELECT * FROM {mysql_config['status_table']} WHERE PTIME = '{target_date}'"

    tmp_dir = "/tmp"
    for file in [
        "modem_data.csv", "modem_status.csv", "node_map.csv", "classified_modems.csv",
        "cm_mapped.csv", "node_score.csv", "inconsistent_nodes_summary.csv",
        "unmapped_modems.csv", "node_ranking_results.csv"
    ]:
        path = f"{tmp_dir}/{file}"
        if os.path.exists(path):
            os.remove(path)

    modem_df = pd.read_sql(modem_query, engine)
    node_map_df = pd.read_sql(node_map_query, engine)
    status_df = pd.read_sql(status_query, engine)

    modem_df.drop_duplicates(inplace=True)
    node_map_df.drop_duplicates(inplace=True)
    status_df.drop_duplicates(inplace=True)

    modem_df.to_csv(f"{tmp_dir}/modem_data.csv", index=False)
    node_map_df.to_csv(f"{tmp_dir}/node_map.csv", index=False)
    status_df.to_csv(f"{tmp_dir}/modem_status.csv", index=False)


def filter_duplicates(tmp_dir="/tmp"):
    modem_df = pd.read_csv(f"{tmp_dir}/modem_data.csv")
    
    # Count occurrences of each CM_MAC
    mac_counts = modem_df["CM_MAC"].value_counts()
    to_exclude = mac_counts[mac_counts > 8].index

    # Filter out those CM_MACs
    filtered_df = modem_df[~modem_df["CM_MAC"].isin(to_exclude)]

    # 👉 Log the number of distinct cable modems after removing duplicates
    unique_modems = filtered_df['CM_MAC'].nunique()
    print(f"📊 Found {unique_modems} unique cable modems after removing cloned.")

    # Overwrite the file
    filtered_df.to_csv(f"{tmp_dir}/modem_data.csv", index=False)

    print(f"🧹 Filtered out {len(to_exclude)} CM_MACs that appeared more than 8 times.")
    print(f"📝 Remaining records: {len(filtered_df)}")



def classify_modems(tmp_dir="/tmp"):
    print("📥 Loading modem and status data...")

    modem_path = f"{tmp_dir}/modem_data.csv"
    status_path = f"{tmp_dir}/modem_status.csv"

    modem_df = pd.read_csv(modem_path)
    status_df = pd.read_csv(status_path)

    print(f"✅ Loaded modem data: {len(modem_df)} rows from {modem_path}")
    print(f"✅ Loaded status data: {len(status_df)} rows from {status_path}")
    print(f"📊 Unique CM_MACs in modem_df: {modem_df['CM_MAC'].nunique()}")
    print(f"📊 Unique CM_MACs in status_df: {status_df['CM_MAC'].nunique()}")

    # Classify each channel
    print("🧮 Classifying each channel...")
    def classify_row(row):
        if row["CER_MAX"] > 1 or row["CCER_MAX"] > 50:
            return "Critical"
        elif row["CER_MAX"] > 0.25 or row["CCER_MAX"] > 5:
            return "Warning"
        else:
            return "Good"

    modem_df["Channel_Status"] = modem_df.apply(classify_row, axis=1)
    print("✅ Channel classification complete.")
    print(modem_df[["CM_MAC", "PTIME", "Channel_Status"]].head())

    # Summarize per CM_MAC and PTIME
    def summarize_group(group):
        if (group["Channel_Status"] == "Critical").any():
            status = "Critical"
        elif (group["Channel_Status"] == "Warning").any():
            status = "Warning"
        else:
            status = "Good"

        partial_service = "Present" if (group["PARTIAL_SERVICE"] != "none(0)").any() else "none(0)"

        return pd.Series({
            "Category": status,
            "PARTIAL_SERVICE": partial_service,
            "CHANNEL_NAME": group["CHANNEL_NAME"].iloc[0],
            "TX_MAX": group["TX_MAX"].max()
        })

    print("📊 Grouping and summarizing modem data...")
    try:
        grouped = modem_df.groupby(["CM_MAC", "PTIME"]).apply(summarize_group)
        print(f"✅ Grouped modem data shape: {grouped.shape}")

        if "PTIME" in grouped.columns:
            print("⚠️ PTIME already in columns, using drop=True in reset_index()")
            modem_level = grouped.reset_index(drop=True)
        else:
            modem_level = grouped.reset_index()

        print("✅ Summarized modem data (head):")
        print(modem_level.head())

    except Exception as e:
        print("❌ Error classifying modems:", e)
        import traceback
        print(traceback.format_exc())
        modem_level = pd.DataFrame()

    if modem_level.empty:
        print("⚠️ No data to classify. Exiting early.")
        return

    print("🔗 Merging modem-level data with status (LEFT join)...")
    merged = modem_level.merge(status_df, on=["CM_MAC", "PTIME"], how="left")
    print(f"✅ Merged dataset shape: {merged.shape}")
    print(f"📊 Rows with no ONLINE_COUNT after merge: {merged['ONLINE_COUNT'].isnull().sum()}")

    print("🛠 Applying final classification rules...")
    merged.loc[merged["ONLINE_COUNT"] == 0, "Category"] = "Offline"
    merged.loc[(merged["ONLINE_COUNT"] != 0) & (merged["RESET_COUNTS"] > 3), "Category"] = "Critical"
    merged.loc[(merged["ONLINE_COUNT"] != 0) & (merged["PARTIAL_SERVICE"] == "Present"), "Category"] = "Critical"

    print("📊 Classification summary after status logic:")
    print(merged["Category"].value_counts(dropna=False))

    output_path = f"{tmp_dir}/classified_modems.csv"
    merged.to_csv(output_path, index=False)
    print(f"💾 Saved classified modems to {output_path}")
    print(f"🔍 Final modem count: {len(merged)} rows")



def verify_node_consistency(tmp_dir="/tmp"):
    node_map = pd.read_csv(f"{tmp_dir}/node_map.csv")

    # Clean data set
    node_map = pd.read_csv(f"{tmp_dir}/node_map.csv").drop_duplicates()
    print('data set cleaned')

    # Count unique CMTS_IDs per FIBER_NODE
    cmts_counts = node_map.groupby("FIBER_NODE")["CMTS_ID"].nunique().reset_index(name="Unique_CMTS_IDs")

    # Count unique sets of CHANNEL_NAMEs per FIBER_NODE
    channel_sets = (
        node_map.groupby(["FIBER_NODE", "CMTS_ID"])["CHANNEL_NAME"]
        .apply(lambda x: tuple(sorted(set(x))))
        .reset_index()
    )
    channel_counts = channel_sets.groupby("FIBER_NODE")["CHANNEL_NAME"].nunique().reset_index(name="Unique_Channel_Sets")

    # Merge both counts
    summary = pd.merge(cmts_counts, channel_counts, on="FIBER_NODE")

    # Filter only inconsistent ones
    inconsistent = summary[(summary["Unique_CMTS_IDs"] > 1) | (summary["Unique_Channel_Sets"] > 1)]

    # Save summary
    inconsistent.to_csv(f"{tmp_dir}/inconsistent_nodes_summary.csv", index=False)

    if inconsistent.empty:
        print("✅ All FIBER_NODEs are consistent.")
    else:
        print("❌ Found inconsistent FIBER_NODEs.")
        print(inconsistent.head())



def map_nodes(tmp_dir="/tmp"):
    print("📍 Loading classified modems and node map...")
    df = pd.read_csv(f"{tmp_dir}/classified_modems.csv")
    node_map = pd.read_csv(f"{tmp_dir}/node_map.csv")

    print(f"📦 Modems before mapping: {len(df)}")

    # Merge based on CMTS_ID and CHANNEL_NAME
    mapped_df = df.merge(
        node_map[["CMTS_ID", "CHANNEL_NAME", "FIBER_NODE"]],
        on=["CMTS_ID", "CHANNEL_NAME"],
        how="left"
    )

    # Check for unmapped rows
    missing_nodes = mapped_df["FIBER_NODE"].isna().sum()
    print(f"⚠️ Modems without node mapping: {missing_nodes}")

    # Save to file
    mapped_df.to_csv(f"{tmp_dir}/cm_mapped.csv", index=False)
    print(f"✅ Saved mapped modem data with node assignment to cm_mapped.csv ({len(mapped_df)} rows)")




def rank_nodes(tmp_dir="/tmp"):
    df = pd.read_csv(f"{tmp_dir}/cm_mapped.csv")

    # Determine the worst case per modem
    df_worst_case = df.groupby("CM_MAC")["Category"].apply(
        lambda x: "Critical" if "Critical" in x.values else
                  ("Warning" if "Warning" in x.values else
                   ("Offline" if "Offline" in x.values else "Good"))
    ).reset_index()

    # Merge to get node info
    df_worst_case = df_worst_case.merge(
        df[["CM_MAC", "FIBER_NODE", "CMTS_ID"]],
        on="CM_MAC",
        how="left"
    ).drop_duplicates()

    # Count categories per node
    df_category_count = df_worst_case.groupby(["FIBER_NODE", "CMTS_ID"])["Category"] \
        .value_counts().unstack(fill_value=0).reset_index()

    # Ensure all categories exist
    for col in ["Good", "Warning", "Critical", "Offline"]:
        if col not in df_category_count.columns:
            df_category_count[col] = 0

    # Total modems per node
    df_category_count["Total_Modems"] = (
        df_category_count["Good"] +
        df_category_count["Warning"] +
        df_category_count["Critical"] +
        df_category_count["Offline"]
    )

    # Max modems (for node size scaling)
    max_modems = df_category_count["Total_Modems"].max()

    # Proportions
    df_category_count["Critical_Rate"] = df_category_count["Critical"] / df_category_count["Total_Modems"]
    df_category_count["Warning_Rate"] = df_category_count["Warning"] / df_category_count["Total_Modems"]

    # Node Size Amplified Penalty (scaled by max size)
    df_category_count["Node_Size_Amplified"] = (
        (df_category_count["Critical"] + df_category_count["Warning"]) *
        (df_category_count["Total_Modems"] / max_modems)
    )

    # Apply weights
    W_CRIT = 45
    W_WARN = 25
    W_SIZE = 30

    df_category_count["Health_Score"] = (
        100
        - W_CRIT * df_category_count["Critical_Rate"]
        - W_WARN * df_category_count["Warning_Rate"]
        - W_SIZE * (df_category_count["Node_Size_Amplified"] / df_category_count["Total_Modems"])
    ).clip(lower=0, upper=100)

    # Rank
    df_category_count = df_category_count.sort_values(by="Health_Score", ascending=True)
    df_category_count["Attention_Priority"] = range(1, len(df_category_count) + 1)

    # Save
    df_category_count.to_csv(f"{tmp_dir}/node_ranking_results.csv", index=False)
    print(f"📊 Ranked {len(df_category_count)} nodes and saved results to node_ranking_results.csv.")



def send_to_elasticsearch(**kwargs):
    import pandas as pd
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    from datetime import datetime, timedelta

    execution_date = kwargs["execution_date"]
    date_str = execution_date.strftime("%Y-%m-%d")

    df = pd.read_csv("/tmp/node_ranking_results.csv")

    # ✅ Read PTIME from modem_data.csv
    modem_df = pd.read_csv("/tmp/modem_data.csv")
    if "PTIME" in modem_df.columns and not modem_df.empty:
        raw_ptime = pd.to_datetime(modem_df["PTIME"].iloc[0])
        ptime = (raw_ptime + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%S")
    else:
        ptime = "unknown"

    print("📋 Columns in ranking file:", df.columns.tolist())
    print("🕓 PTIME used for this run:", ptime)

    ES_HOST = "https://10.212.56.57:9200"
    ES_USER = "elastic"
    ES_PASS = "Broadbus1"
    ES_INDEX = "hs_lpr"

    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USER, ES_PASS),
        verify_certs=False
    )

    actions = []
    for _, row in df.iterrows():
        actions.append({
            "_index": ES_INDEX,
            "_source": {
                **row.to_dict(),
                "PTIME": ptime  # ✅ Adjusted PTIME with 12:00 to avoid timezone rollback
            }
        })

    try:
        success, _ = bulk(es, actions)
        print(f"✅ Sent {success} documents to Elasticsearch index '{ES_INDEX}'")
    except Exception as e:
        print(f"❌ Elasticsearch bulk insert failed: {e}")
        
        
# ========== DAG Definition ==========
local_tz = timezone("America/Mexico_City")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': local_tz.datetime(2025, 5, 18, 4, 0, 0),
    'end_date': local_tz.datetime(2025, 11, 22, 4, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'libertyPr_health_score_1',
    default_args=default_args,
    description='Single-file version of health score DAG for LibertyPR',
    schedule_interval='0 4 * * *',  # run every day at 06:00
    max_active_runs=1,
    catchup=True,
    tags=['health_score'],
)
# ========== Tasks ==========

load = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

log_dates = PythonOperator(
    task_id="log_dates",
    python_callable=log_airflow_dates,
    provide_context=True,
    dag=dag,
)

filter_dup = PythonOperator(
    task_id="filter_duplicates",
    python_callable=filter_duplicates,
    dag=dag
)

classify = PythonOperator(
    task_id="classify_modems",
    python_callable=classify_modems,
    dag=dag
)

verify = PythonOperator(
    task_id="verify_node_consistency",
    python_callable=verify_node_consistency,
    dag=dag
)

mapn = PythonOperator(
    task_id="map_nodes",
    python_callable=map_nodes,
    dag=dag
)

rank = PythonOperator(
    task_id="rank_nodes",
    python_callable=rank_nodes,
    dag=dag
)

send_es = PythonOperator(
    task_id="send_to_elasticsearch",
    python_callable=send_to_elasticsearch,
    provide_context=True,
    dag=dag
)

load >> log_dates >> filter_dup >> classify >> verify >> mapn >> rank >> send_es

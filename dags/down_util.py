from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch, helpers

DB_URL = "mysql+pymysql://cabletica_dataeng_views:cabletica_dataeng@172.18.3.232:3306/cabletica?ssl=true&ssl_verify_cert=false"
ES_HOST = "https://192.168.102.20:9200"
ES_INDEX = "downstream_utilization"

def extract_and_push_to_es(**kwargs):
    print("📡 Starting downstream utilization query task...")

    query = """
    SELECT
        HUB_ID,
        CMTS_ID,
        CABLE_MAC,
        CONCAT(CMTS_ID, ' ', CABLE_MAC) AS G_DSG,
        SCQAM_UTILIZATION,
        START_TIME
    FROM
        cabletica.DOWN_SG_UTIL_FACTS
    WHERE
        START_TIME BETWEEN
            (SELECT MAX(START_TIME) - INTERVAL 14 MINUTE FROM cabletica.DOWN_SG_UTIL_FACTS)
            AND
            (SELECT MAX(START_TIME) FROM cabletica.DOWN_SG_UTIL_FACTS)
    """

    print("🔌 Connecting to MySQL...")
    engine = create_engine(DB_URL)

    try:
        df = pd.read_sql(query, engine)
        print(f"✅ Retrieved {len(df)} rows from MySQL.")
    except Exception as e:
        print(f"❌ Failed to query MySQL: {e}")
        raise

    if df.empty:
        print("⚠️ No data found above threshold. Skipping ES push.")
        return

    print("🔗 Connecting to Elasticsearch...")
    try:
        es = Elasticsearch(ES_HOST, basic_auth=("elastic", "Ben-fong-torres-3"), verify_certs=False)
        print("✅ Connected to ES")
    except Exception as e:
        print(f"❌ Failed to connect to ES: {e}")
        raise

    actions = []
    for i, row in df.iterrows():
        try:
            doc = {
                "@timestamp": row["START_TIME"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "hub_id": row["HUB_ID"],
                "G_DSG": row["G_DSG"],
                "scqam_utilization": float(row["SCQAM_UTILIZATION"]),
            }
            actions.append({"_index": ES_INDEX, "_source": doc})
        except Exception as e:
            print(f"⚠️ Error processing row {i}: {e}")

    print(f"📦 Prepared {len(actions)} documents to push.")

    if not actions:
        print("⚠️ No documents prepared for Elasticsearch.")
        return

    try:
        result = helpers.bulk(es, actions)
        print(f"✅ Successfully pushed to ES index '{ES_INDEX}': {result}")
    except Exception as e:
        print(f"❌ Bulk insert to ES failed: {e}")
        raise

with DAG(
    dag_id="downstream_util_dag",
    schedule_interval="*/15 * * * *",
    start_date=datetime.today(),
    catchup=False,
    max_active_runs=1,
    tags=["downstream", "elasticsearch"]
) as dag:

    push_task = PythonOperator(
        task_id="extract_and_push",
        python_callable=extract_and_push_to_es,
        provide_context=True
    )

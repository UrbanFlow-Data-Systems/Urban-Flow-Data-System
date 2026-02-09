from airflow.decorators import task
from airflow.utils.context import Context
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os
import logging

logger = logging.getLogger(__name__)

@task
def generate_traffic_volume_vs_time_report(**context):
    """
    Analytic Report:
    Traffic Volume vs Time of Day
    """

    # --------------------------------------------------
    # 1. Get analysis date from XCom
    # --------------------------------------------------
    ti = context['ti']
    analysis_date = ti.xcom_pull(
        key='analysis_date',
        task_ids='extract_daily_traffic_data'
    )

    if not analysis_date:
        logger.warning("No analysis date found. Skipping analytic report.")
        return "No analysis date"

    logger.info(f"Generating Traffic Volume vs Time report for {analysis_date}")

    # --------------------------------------------------
    # 2. Database connection
    # --------------------------------------------------
    db_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/traffic_db"
    engine = create_engine(db_url)

    # --------------------------------------------------
    # 3. Query traffic volume by hour
    # --------------------------------------------------
    query = f"""
        SELECT
            hour,
            SUM(total_vehicles) AS traffic_volume
        FROM aggregated_stats
        WHERE date = '{analysis_date}'
        GROUP BY hour
        ORDER BY hour;
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        logger.warning("No data available for Traffic Volume vs Time report.")
        return "No data for analytic report"

    # --------------------------------------------------
    # 4. Ensure output directory exists
    # --------------------------------------------------
    output_dir = "/opt/airflow/reports/analytics"
    os.makedirs(output_dir, exist_ok=True)

    # --------------------------------------------------
    # 5. Save table as CSV
    # --------------------------------------------------
    csv_path = f"{output_dir}/traffic_volume_vs_time_{analysis_date}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved analytic table: {csv_path}")

    # --------------------------------------------------
    # 6. Generate visualization
    # --------------------------------------------------
    plt.figure(figsize=(10, 6))
    plt.plot(df['hour'], df['traffic_volume'], marker='o')
    plt.title(f"Traffic Volume vs Time of Day ({analysis_date})")
    plt.xlabel("Hour of Day")
    plt.ylabel("Total Vehicles")
    plt.xticks(range(0, 24))
    plt.grid(True)

    img_path = f"{output_dir}/traffic_volume_vs_time_{analysis_date}.png"
    plt.savefig(img_path)
    plt.close()

    logger.info(f"Saved analytic visualization: {img_path}")

    return {
        "date": analysis_date,
        "csv": csv_path,
        "image": img_path
    }

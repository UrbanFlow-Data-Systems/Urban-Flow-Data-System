"""
Smart City Traffic - Airflow DAG
Nightly batch job for peak traffic analysis and intervention report generation
Schedule: Daily at 1:00 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'smartcity',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'smart_city_daily_traffic_analysis',
    default_args=default_args,
    description='Daily analysis of traffic patterns and intervention recommendations',
    schedule_interval='0 1 * * *',  # Run at 1:00 AM daily
    catchup=False,
    tags=['traffic', 'analytics', 'batch']
)


def extract_daily_traffic_data(**context):
    """
    Task 1: Extract previous day's traffic data from PostgreSQL
    """
    execution_date = context['execution_date']
    analysis_date = (execution_date - timedelta(days=1)).date()
    
    logger.info(f"📊 Extracting traffic data for: {analysis_date}")
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic_db')
    
    # Query to get raw traffic data for the previous day
    query = f"""
    SELECT 
        sensor_id,
        timestamp,
        vehicle_count,
        avg_speed,
        EXTRACT(HOUR FROM timestamp) as hour
    FROM traffic_events
    WHERE DATE(timestamp) = '{analysis_date}'
    ORDER BY sensor_id, timestamp
    """
    
    df = pg_hook.get_pandas_df(query)
    
    if df.empty:
        logger.warning(f"⚠️  No data found for {analysis_date}")
        return None
    
    logger.info(f"✅ Extracted {len(df)} records")
    
    # Save to XCom for next task
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    context['ti'].xcom_push(key='analysis_date', value=str(analysis_date))
    
    return len(df)

# def create_tables_if_not_exist():
#     conn = psycopg2.connect(**db_config)
#     cursor = conn.cursor()
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS aggregated_stats (
#             id SERIAL PRIMARY KEY,
#             sensor_id INT NOT NULL,
#             hour INT NOT NULL,
#             avg_vehicle_count FLOAT,
#             avg_speed FLOAT,
#             total_vehicles INT,
#             congestion_events INT,
#             date DATE NOT NULL,
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#     """)
#     conn.commit()
#     cursor.close()
#     conn.close()

def aggregate_hourly_statistics(**context):
    """
    Task 2: Aggregate traffic data by sensor and hour
    """
    ti = context['ti']
    raw_data_json = ti.xcom_pull(key='raw_data', task_ids='extract_daily_data')
    analysis_date = ti.xcom_pull(key='analysis_date', task_ids='extract_daily_data')
    
    if not raw_data_json:
        logger.warning("⚠️  No data to aggregate")
        return None
    
    logger.info(f"📈 Aggregating hourly statistics for {analysis_date}")
    
    # Load data from XCom
    df = pd.read_json(raw_data_json)
    
    # Aggregate by sensor and hour
    hourly_stats = df.groupby(['sensor_id', 'hour']).agg({
        'vehicle_count': ['mean', 'sum'],
        'avg_speed': 'mean'
    }).reset_index()
    
    hourly_stats.columns = ['sensor_id', 'hour', 'avg_vehicle_count', 
                            'total_vehicles', 'avg_speed']
    
    # Count congestion events (speed < 10 km/h)
    congestion_counts = df[df['avg_speed'] < 10].groupby(['sensor_id', 'hour']).size()
    congestion_counts = congestion_counts.reset_index(name='congestion_events')
    
    # Merge with hourly stats
    hourly_stats = hourly_stats.merge(
        congestion_counts, 
        on=['sensor_id', 'hour'], 
        how='left'
    )
    hourly_stats['congestion_events'] = hourly_stats['congestion_events'].fillna(0).astype(int)
    
    logger.info(f"✅ Aggregated {len(hourly_stats)} hourly records")
    
    # Save aggregated data to database
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Insert aggregated statistics
    for _, row in hourly_stats.iterrows():
        cursor.execute("""
            INSERT INTO aggregated_stats 
            (sensor_id, date, hour, avg_vehicle_count, avg_speed, 
             total_vehicles, congestion_events)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sensor_id, date, hour) 
            DO UPDATE SET
                avg_vehicle_count = EXCLUDED.avg_vehicle_count,
                avg_speed = EXCLUDED.avg_speed,
                total_vehicles = EXCLUDED.total_vehicles,
                congestion_events = EXCLUDED.congestion_events
        """, (
            row['sensor_id'],
            analysis_date,
            int(row['hour']),
            float(row['avg_vehicle_count']),
            float(row['avg_speed']),
            int(row['total_vehicles']),
            int(row['congestion_events'])
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info("✅ Aggregated statistics saved to database")
    
    # Pass to next task
    ti.xcom_push(key='hourly_stats', value=hourly_stats.to_json())
    
    return len(hourly_stats)


def identify_peak_hours(**context):
    """
    Task 3: Identify peak traffic hours for each junction
    """
    ti = context['ti']
    hourly_stats_json = ti.xcom_pull(key='hourly_stats', task_ids='aggregate_hourly_stats')
    analysis_date = ti.xcom_pull(key='analysis_date', task_ids='extract_daily_data')
    
    if not hourly_stats_json:
        logger.warning("⚠️  No hourly statistics available")
        return None
    
    logger.info(f"🔍 Identifying peak traffic hours for {analysis_date}")
    
    df = pd.read_json(hourly_stats_json)
    
    # Find peak hour for each sensor (highest total vehicles)
    peak_hours = df.loc[df.groupby('sensor_id')['total_vehicles'].idxmax()]
    
    # Determine intervention requirement
    # Criteria: High congestion events OR very high vehicle count OR very low speed
    peak_hours['requires_intervention'] = (
        (peak_hours['congestion_events'] >= 5) |
        (peak_hours['total_vehicles'] > df['total_vehicles'].quantile(0.75)) |
        (peak_hours['avg_speed'] < 15)
    )
    
    # Calculate intervention priority (1=highest, 4=lowest)
    peak_hours['intervention_priority'] = peak_hours.apply(
        lambda row: (
            1 if row['congestion_events'] >= 10 else
            2 if row['avg_speed'] < 10 else
            3 if row['total_vehicles'] > df['total_vehicles'].quantile(0.9) else
            4
        ) if row['requires_intervention'] else None,
        axis=1
    )
    
    logger.info(f"✅ Identified peak hours for {len(peak_hours)} junctions")
    
    # Save to database
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    for _, row in peak_hours.iterrows():
        cursor.execute("""
            INSERT INTO peak_traffic_analysis
            (sensor_id, analysis_date, peak_hour, peak_vehicle_count,
             avg_peak_speed, requires_intervention, intervention_priority)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sensor_id, analysis_date)
            DO UPDATE SET
                peak_hour = EXCLUDED.peak_hour,
                peak_vehicle_count = EXCLUDED.peak_vehicle_count,
                avg_peak_speed = EXCLUDED.avg_peak_speed,
                requires_intervention = EXCLUDED.requires_intervention,
                intervention_priority = EXCLUDED.intervention_priority
        """, (
            row['sensor_id'],
            analysis_date,
            int(row['hour']),
            int(row['total_vehicles']),
            float(row['avg_speed']),
            bool(row['requires_intervention']),
            int(row['intervention_priority']) if pd.notna(row['intervention_priority']) else None
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info("✅ Peak traffic analysis saved to database")
    
    # Pass to report generation
    ti.xcom_push(key='peak_hours', value=peak_hours.to_json())
    
    return peak_hours['requires_intervention'].sum()


def generate_intervention_report(**context):
    """
    Task 4: Generate intervention report for traffic police
    """
    ti = context['ti']
    peak_hours_json = ti.xcom_pull(key='peak_hours', task_ids='identify_peak_hours')
    analysis_date = ti.xcom_pull(key='analysis_date', task_ids='extract_daily_data')
    
    if not peak_hours_json:
        logger.warning("⚠️  No peak hour data available")
        return None
    
    logger.info(f"📄 Generating intervention report for {analysis_date}")
    
    df = pd.read_json(peak_hours_json)
    
    # Get junction names
    pg_hook = PostgresHook(postgres_conn_id='postgres_traffic_db')
    junctions = pg_hook.get_pandas_df("SELECT sensor_id, junction_name, location FROM junctions")
    
    df = df.merge(junctions, on='sensor_id', how='left')
    
    # Filter junctions requiring intervention
    intervention_df = df[df['requires_intervention'] == True].sort_values('intervention_priority')
    
    # Generate report
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("SMART CITY TRAFFIC MANAGEMENT - DAILY INTERVENTION REPORT")
    report_lines.append("=" * 80)
    report_lines.append(f"Analysis Date: {analysis_date}")
    report_lines.append(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    if len(intervention_df) == 0:
        report_lines.append("✅ NO INTERVENTIONS REQUIRED")
        report_lines.append("All junctions are operating within normal parameters.")
    else:
        report_lines.append(f"🚨 INTERVENTIONS REQUIRED: {len(intervention_df)} Junction(s)")
        report_lines.append("")
        
        for idx, row in intervention_df.iterrows():
            priority_label = {1: "CRITICAL", 2: "HIGH", 3: "MEDIUM", 4: "LOW"}
            report_lines.append(f"Priority {row['intervention_priority']}: {priority_label.get(row['intervention_priority'], 'N/A')}")
            report_lines.append(f"  Junction: {row['junction_name']}")
            report_lines.append(f"  Location: {row['location']}")
            report_lines.append(f"  Peak Hour: {int(row['hour'])}:00")
            report_lines.append(f"  Total Vehicles: {int(row['total_vehicles'])}")
            report_lines.append(f"  Average Speed: {row['avg_speed']:.2f} km/h")
            report_lines.append(f"  Congestion Events: {int(row['congestion_events'])}")
            report_lines.append("")
    
    report_lines.append("=" * 80)
    report_lines.append("SUMMARY STATISTICS - ALL JUNCTIONS")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    for _, row in df.iterrows():
        status = "⚠️ REQUIRES ATTENTION" if row['requires_intervention'] else "✅ NORMAL"
        report_lines.append(f"{row['junction_name']}: {status}")
        report_lines.append(f"  Peak: {int(row['hour'])}:00 | Vehicles: {int(row['total_vehicles'])} | Speed: {row['avg_speed']:.2f} km/h")
        report_lines.append("")
    
    report_lines.append("=" * 80)
    report_lines.append("END OF REPORT")
    report_lines.append("=" * 80)
    
    report_text = "\n".join(report_lines)
    
    # Save report to file
    report_filename = f"/opt/airflow/reports/intervention_report_{analysis_date}.txt"
    with open(report_filename, 'w') as f:
        f.write(report_text)
    
    logger.info(f"✅ Report saved: {report_filename}")
    
    # Also log to console
    print("\n" + report_text)
    
    return report_filename


# Define tasks
task_extract = PythonOperator(
    task_id='extract_daily_data',
    python_callable=extract_daily_traffic_data,
    dag=dag
)

task_aggregate = PythonOperator(
    task_id='aggregate_hourly_stats',
    python_callable=aggregate_hourly_statistics,
    dag=dag
)

task_peak_hours = PythonOperator(
    task_id='identify_peak_hours',
    python_callable=identify_peak_hours,
    dag=dag
)

task_report = PythonOperator(
    task_id='generate_intervention_report',
    python_callable=generate_intervention_report,
    dag=dag
)
# create_tables = PythonOperator(
#     task_id='create_tables',
#     python_callable=create_tables_if_not_exist,
#     dag=dag,
# )
# Define task dependencies
task_extract >>  task_aggregate >> task_peak_hours >> task_report
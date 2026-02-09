
import psycopg2
import pandas as pd
import matplotlib
matplotlib.use("Agg")  
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

import warnings
warnings.filterwarnings('ignore')

# Configure plot style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)
plt.rcParams['font.size'] = 10

def get_database_connection():
    """Establish PostgreSQL connection"""
    hook = PostgresHook(postgres_conn_id="postgres_traffic_db")
    return hook.get_conn()


def fetch_traffic_data(conn, analysis_date=None):

    if analysis_date is None:
        analysis_date = (datetime.now() - timedelta(days=1)).date()
    
    query = f"""
    SELECT 
        agg.sensor_id,
        j.junction_name,
        agg.hour,
        agg.avg_vehicle_count,
        agg.avg_speed,
        agg.total_vehicles,
        agg.congestion_events
    FROM aggregated_stats agg
    JOIN junctions j ON agg.sensor_id::text = j.sensor_id::text
    WHERE agg.date = '{analysis_date}'
    ORDER BY agg.sensor_id, agg.hour
    """
    
    df = pd.read_sql(query, conn)
    print(f"Fetched {len(df)} records for {analysis_date}")
    return df


def fetch_peak_analysis(conn, analysis_date=None):
    """Fetch peak traffic analysis results"""
    if analysis_date is None:
        analysis_date = (datetime.now() - timedelta(days=1)).date()
    
    query = f"""
    SELECT 
        p.sensor_id,
        j.junction_name,
        p.peak_hour,
        p.peak_vehicle_count,
        p.avg_peak_speed,
        p.requires_intervention,
        p.intervention_priority
    FROM peak_traffic_analysis p
    JOIN junctions j ON p.sensor_id::text = j.sensor_id::text
    WHERE p.analysis_date = '{analysis_date}'
    ORDER BY p.intervention_priority NULLS LAST, p.peak_vehicle_count DESC
    """
    
    df = pd.read_sql(query, conn)
    print(f"Fetched peak analysis for {len(df)} junctions")
    return df


def create_traffic_volume_chart(df, analysis_date):
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle(f'Traffic Volume vs Time of Day - {analysis_date}', 
                 fontsize=16, fontweight='bold', y=0.995)
    
    junctions = df['junction_name'].unique()
    
    for idx, junction in enumerate(junctions):
        ax = axes[idx // 2, idx % 2]
        junction_data = df[df['junction_name'] == junction]
        
        # Primary axis - Vehicle Count
        color = 'tab:blue'
        ax.set_xlabel('Hour of Day', fontweight='bold')
        ax.set_ylabel('Total Vehicles', color=color, fontweight='bold')
        ax.bar(junction_data['hour'], junction_data['total_vehicles'], 
               color=color, alpha=0.6, label='Total Vehicles')
        ax.tick_params(axis='y', labelcolor=color)
        ax.set_xticks(range(0, 24, 2))
        ax.grid(True, alpha=0.3)
        
        # Secondary axis - Average Speed
        ax2 = ax.twinx()
        color = 'tab:orange'
        ax2.set_ylabel('Average Speed (km/h)', color=color, fontweight='bold')
        ax2.plot(junction_data['hour'], junction_data['avg_speed'], 
                color=color, linewidth=2, marker='o', label='Avg Speed')
        ax2.tick_params(axis='y', labelcolor=color)
        
        # Highlight congestion events
        congested_hours = junction_data[junction_data['congestion_events'] > 0]
        for _, row in congested_hours.iterrows():
            ax.axvspan(row['hour'] - 0.4, row['hour'] + 0.4, 
                      color='red', alpha=0.2)
        
        # Title and legend
        ax.set_title(f'{junction}', fontweight='bold', pad=10)
        
        # Combine legends
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    return fig


def create_congestion_heatmap(df, analysis_date):
    """Create congestion heatmap across junctions and hours"""
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Prepare data for heatmap
    pivot_data = df.pivot(index='junction_name', 
                          columns='hour', 
                          values='congestion_events')
    pivot_data = pivot_data.fillna(0)
    
    # Create heatmap
    sns.heatmap(pivot_data, annot=True, fmt='.0f', cmap='YlOrRd', 
                cbar_kws={'label': 'Congestion Events'}, ax=ax)
    
    ax.set_title(f'Congestion Events Heatmap - {analysis_date}', 
                 fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Hour of Day', fontweight='bold')
    ax.set_ylabel('Junction', fontweight='bold')
    
    plt.tight_layout()
    return fig


def create_speed_analysis_chart(df, analysis_date):
    """Create average speed analysis chart"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Chart 1: Average speed by junction over time
    for junction in df['junction_name'].unique():
        junction_data = df[df['junction_name'] == junction]
        ax1.plot(junction_data['hour'], junction_data['avg_speed'], 
                marker='o', linewidth=2, label=junction)
    
    ax1.axhline(y=10, color='red', linestyle='--', linewidth=2, 
                label='Critical Threshold (10 km/h)')
    ax1.set_xlabel('Hour of Day', fontweight='bold')
    ax1.set_ylabel('Average Speed (km/h)', fontweight='bold')
    ax1.set_title('Average Speed Throughout the Day', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(range(0, 24, 2))
    
    # Chart 2: Average speed distribution by junction
    junction_avg_speeds = df.groupby('junction_name')['avg_speed'].mean().sort_values()
    colors = ['red' if x < 15 else 'orange' if x < 25 else 'green' 
              for x in junction_avg_speeds.values]
    
    junction_avg_speeds.plot(kind='barh', ax=ax2, color=colors)
    ax2.set_xlabel('Average Speed (km/h)', fontweight='bold')
    ax2.set_ylabel('Junction', fontweight='bold')
    ax2.set_title('Daily Average Speed by Junction', fontweight='bold')
    ax2.axvline(x=10, color='red', linestyle='--', linewidth=2, alpha=0.5)
    ax2.grid(True, alpha=0.3, axis='x')
    
    plt.suptitle(f'Speed Analysis - {analysis_date}', 
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    return fig


def create_intervention_summary(peak_df, analysis_date):
    """Create intervention priority summary chart"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Chart 1: Peak vehicle counts
    peak_df_sorted = peak_df.sort_values('peak_vehicle_count', ascending=True)
    colors = ['red' if x else 'green' for x in peak_df_sorted['requires_intervention']]
    
    ax1.barh(peak_df_sorted['junction_name'], 
             peak_df_sorted['peak_vehicle_count'], 
             color=colors, alpha=0.7)
    ax1.set_xlabel('Peak Vehicle Count', fontweight='bold')
    ax1.set_ylabel('Junction', fontweight='bold')
    ax1.set_title('Peak Hour Vehicle Count', fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='x')
    
    # Add intervention markers
    for idx, row in peak_df_sorted.iterrows():
        if row['requires_intervention']:
            ax1.text(row['peak_vehicle_count'] + 50, idx, 
                    f"P{int(row['intervention_priority'])}", 
                    va='center', fontweight='bold', color='red')
    
    # Chart 2: Intervention requirements
    intervention_counts = peak_df['requires_intervention'].value_counts()
    labels = ['Requires Intervention' if val else 'Normal Operation' 
          for val in intervention_counts.index]
    colors_pie = ['red' if val else 'green' for val in intervention_counts.index]

    ax2.pie(intervention_counts.values, labels=labels, autopct='%1.1f%%',
        colors=colors_pie, startangle=90, textprops={'fontweight': 'bold'})

    ax2.set_title('Intervention Requirements', fontweight='bold')
    
    plt.suptitle(f'Intervention Analysis - {analysis_date}', 
                 fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    return fig


def generate_summary_table(df, peak_df, analysis_date):
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.axis('tight')
    ax.axis('off')
    
    # Prepare summary data
    summary_data = []
    for _, row in peak_df.iterrows():
        junction_data = df[df['junction_name'] == row['junction_name']]
        
        summary_data.append([
            row['junction_name'],
            f"{row['peak_hour']:02d}:00",
            int(row['peak_vehicle_count']),
            f"{row['avg_peak_speed']:.1f}",
            int(junction_data['congestion_events'].sum()),
            "YES" if row['requires_intervention'] else "NO",
            f"P{int(row['intervention_priority'])}" if row['requires_intervention'] else "-"
        ])
    
    # Create table
    columns = ['Junction', 'Peak Hour', 'Peak Vehicles', 
               'Avg Speed (km/h)', 'Congestion Events', 
               'Intervention Required', 'Priority']
    
    table = ax.table(cellText=summary_data, colLabels=columns,
                    cellLoc='center', loc='center',
                    colColours=['lightblue'] * len(columns))
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Color code intervention rows
    for i, row_data in enumerate(summary_data):
        if row_data[5] == "YES":
            for j in range(len(columns)):
                table[(i+1, j)].set_facecolor('#ffcccc')
    
    plt.title(f'Daily Traffic Summary - {analysis_date}', 
              fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    return fig


def generate_visualization_report(**context):
    # analysis_date = context['ti'].xcom_pull(
    #     key='analysis_date',
    #     task_ids='extract_daily_data'
    # )
    analysis_date = "2026-02-09"


    conn = get_database_connection()

    traffic_df = fetch_traffic_data(conn, analysis_date)
    peak_df = fetch_peak_analysis(conn, analysis_date)

    if traffic_df.empty:
        return "No data for visualization"

    output_dir = "/opt/airflow/reports/visualizations"
    os.makedirs(output_dir, exist_ok=True)

    fig1 = create_traffic_volume_chart(traffic_df, analysis_date)
    fig1.savefig(f"{output_dir}/traffic_volume_{analysis_date}.png", dpi=300)
    # fig1.close()

    fig2 = create_congestion_heatmap(traffic_df, analysis_date)
    fig2.savefig(f"{output_dir}/congestion_heatmap_{analysis_date}.png", dpi=300)
    # fig2.close()

    fig3 = create_speed_analysis_chart(traffic_df, analysis_date)
    fig3.savefig(f"{output_dir}/speed_analysis_{analysis_date}.png", dpi=300)
    # fig3.close()
    if not peak_df.empty:
        fig4 = create_intervention_summary(peak_df, analysis_date)
        fig4.savefig(f"{output_dir}/intervention_summary_{analysis_date}.png", dpi=300)
        # fig4.close()
    
        fig5 = generate_summary_table(traffic_df, peak_df, analysis_date)
        fig5.savefig(f"{output_dir}/summary_table_{analysis_date}.png", dpi=300)
        # fig5.close()
    
    conn.close()
    return "Visualization report generated"

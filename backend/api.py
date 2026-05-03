"""
Flask API Backend for Smart City Traffic Dashboard
Provides REST endpoints for real-time traffic data visualization
"""

from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'traffic_db',
    'user': 'smartcity',
    'password': 'smartcity123'
}

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

def serialize_datetime(obj):
    """JSON serializer for datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/', methods=['GET'])
def index():
    """Root endpoint with quick API guide"""
    return jsonify({
        'service': 'Smart City Traffic API',
        'status': 'running',
        'endpoints': [
            '/api/health',
            '/api/traffic/current',
            '/api/traffic/timeseries/<sensor_id>',
            '/api/traffic/timeseries/all',
            '/api/alerts/active',
            '/api/statistics/summary',
            '/api/statistics/hourly',
            '/api/junctions'
        ],
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/traffic/current', methods=['GET'])
def get_current_traffic():
    """Get current traffic status for all junctions (last 5 minutes)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        WITH latest_readings AS (
            SELECT DISTINCT ON (sensor_id)
                sensor_id,
                timestamp,
                vehicle_count,
                avg_speed
            FROM traffic_events
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            ORDER BY sensor_id, timestamp DESC
        )
        SELECT 
            j.sensor_id,
            j.junction_name,
            j.location,
            j.latitude,
            j.longitude,
            lr.timestamp,
            lr.vehicle_count,
            lr.avg_speed,
            CASE 
                WHEN lr.avg_speed < 10 THEN 'CRITICAL'
                WHEN lr.avg_speed < 20 THEN 'HIGH'
                WHEN lr.avg_speed < 40 THEN 'MODERATE'
                ELSE 'NORMAL'
            END as status
        FROM junctions j
        LEFT JOIN latest_readings lr ON j.sensor_id = lr.sensor_id
        ORDER BY j.junction_name
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/traffic/timeseries/<sensor_id>', methods=['GET'])
def get_traffic_timeseries(sensor_id):
    """Get time series data for a specific sensor (last hour)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            timestamp,
            vehicle_count,
            avg_speed
        FROM traffic_events
        WHERE sensor_id = %s
        AND timestamp >= NOW() - INTERVAL '1 hour'
        ORDER BY timestamp ASC
        """
        
        cursor.execute(query, (sensor_id,))
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'sensor_id': sensor_id,
            'data': results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/traffic/timeseries/all', methods=['GET'])
def get_all_traffic_timeseries():
    """Get time series data for all sensors (last 30 minutes)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            te.sensor_id,
            j.junction_name,
            te.timestamp,
            te.vehicle_count,
            te.avg_speed
        FROM traffic_events te
        JOIN junctions j ON te.sensor_id = j.sensor_id
        WHERE te.timestamp >= NOW() - INTERVAL '30 minutes'
        ORDER BY te.timestamp ASC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Group by sensor_id
        grouped_data = {}
        for row in results:
            sensor_id = row['sensor_id']
            if sensor_id not in grouped_data:
                grouped_data[sensor_id] = {
                    'sensor_id': sensor_id,
                    'junction_name': row['junction_name'],
                    'data': []
                }
            grouped_data[sensor_id]['data'].append({
                'timestamp': row['timestamp'].isoformat(),
                'vehicle_count': row['vehicle_count'],
                'avg_speed': row['avg_speed']
            })
        
        return jsonify({
            'success': True,
            'data': list(grouped_data.values()),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/alerts/active', methods=['GET'])
def get_active_alerts():
    """Get active congestion alerts (last 15 minutes)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            ca.id,
            ca.sensor_id,
            j.junction_name,
            ca.alert_timestamp,
            ca.window_start,
            ca.window_end,
            ca.avg_speed,
            ca.total_vehicles,
            ca.congestion_index,
            ca.severity
        FROM congestion_alerts ca
        JOIN junctions j ON ca.sensor_id = j.sensor_id
        WHERE ca.alert_timestamp >= NOW() - INTERVAL '15 minutes'
        ORDER BY ca.alert_timestamp DESC
        LIMIT 50
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': results,
            'count': len(results),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/statistics/summary', methods=['GET'])
def get_statistics_summary():
    """Get summary statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Total events today
        cursor.execute("""
            SELECT COUNT(*) as total_events
            FROM traffic_events
            WHERE DATE(timestamp) = CURRENT_DATE
        """)
        total_events = cursor.fetchone()['total_events']
        
        # Active alerts (last hour)
        cursor.execute("""
            SELECT COUNT(*) as active_alerts
            FROM congestion_alerts
            WHERE alert_timestamp >= NOW() - INTERVAL '1 hour'
        """)
        active_alerts = cursor.fetchone()['active_alerts']
        
        # Average speed across all junctions (last 5 minutes)
        cursor.execute("""
            SELECT AVG(avg_speed) as avg_speed
            FROM traffic_events
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
        """)
        avg_speed_result = cursor.fetchone()
        avg_speed = round(avg_speed_result['avg_speed'], 2) if avg_speed_result['avg_speed'] else 0
        
        # Critical junctions (speed < 10 in last 5 minutes)
        cursor.execute("""
            SELECT COUNT(DISTINCT sensor_id) as critical_junctions
            FROM traffic_events
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            AND avg_speed < 10
        """)
        critical_junctions = cursor.fetchone()['critical_junctions']
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'total_events_today': total_events,
                'active_alerts': active_alerts,
                'average_speed': avg_speed,
                'critical_junctions': critical_junctions
            },
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/statistics/hourly', methods=['GET'])
def get_hourly_statistics():
    """Get hourly statistics for today"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            EXTRACT(HOUR FROM timestamp) as hour,
            COUNT(*) as event_count,
            AVG(vehicle_count) as avg_vehicles,
            AVG(avg_speed) as avg_speed,
            COUNT(CASE WHEN avg_speed < 10 THEN 1 END) as critical_events
        FROM traffic_events
        WHERE DATE(timestamp) = CURRENT_DATE
        GROUP BY EXTRACT(HOUR FROM timestamp)
        ORDER BY hour
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/junctions', methods=['GET'])
def get_junctions():
    """Get all junction information"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        SELECT 
            sensor_id,
            junction_name,
            location,
            latitude,
            longitude
        FROM junctions
        ORDER BY junction_name
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
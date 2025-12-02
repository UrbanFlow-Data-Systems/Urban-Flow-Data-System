CREATE TABLE IF NOT EXISTS aggregated_stats (
    sensor_id VARCHAR(50),
    date DATE,
    hour INT,
    avg_vehicle_count FLOAT,
    avg_speed FLOAT,
    total_vehicles INT,
    congestion_events INT,
    PRIMARY KEY (sensor_id, date, hour)
);

CREATE TABLE IF NOT EXISTS peak_traffic_analysis (
    sensor_id VARCHAR(50),
    analysis_date DATE,
    peak_hour INT,
    peak_vehicle_count INT,
    avg_peak_speed FLOAT,
    requires_intervention BOOLEAN,
    intervention_priority INT,
    PRIMARY KEY (sensor_id, analysis_date)
);

CREATE TABLE IF NOT EXISTS junctions (
    sensor_id VARCHAR(50) PRIMARY KEY,
    junction_name VARCHAR(255),
    location VARCHAR(255)
);

CREATE TABLE traffic_data (
  sensor_id VARCHAR(50),
  timestamp TIMESTAMP,
  vehicle_count INT,
  avg_speed FLOAT
);

CREATE TABLE critical_alerts (
  sensor_id VARCHAR(50),
  timestamp TIMESTAMP,
  avg_speed FLOAT,
  message TEXT
);

CREATE TABLE daily_summary (
  date DATE,
  sensor_id VARCHAR(50),
  peak_hour INT,
  total_vehicles INT
);

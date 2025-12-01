"""
Smart City Traffic Sensor Data Producer
Simulates 4 traffic sensors at different junctions in Colombo
Generates realistic traffic data with occasional congestion events
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Sensor configuration
SENSORS = [
    {
        'sensor_id': 'SENSOR_GALLE_ROAD',
        'junction_name': 'Galle Road Junction',
        'base_vehicle_count': 45,
        'base_speed': 35
    },
    {
        'sensor_id': 'SENSOR_BASELINE_ROAD',
        'junction_name': 'Baseline Road Junction',
        'base_vehicle_count': 38,
        'base_speed': 40
    },
    {
        'sensor_id': 'SENSOR_DUPLICATION_ROAD',
        'junction_name': 'Duplication Road Junction',
        'base_vehicle_count': 52,
        'base_speed': 30
    },
    {
        'sensor_id': 'SENSOR_KANDY_ROAD',
        'junction_name': 'Kandy Road Junction',
        'base_vehicle_count': 42,
        'base_speed': 38
    }
]

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'traffic-data'


class TrafficDataGenerator:
    """Generates realistic traffic data with peak hours and congestion events"""
    
    def __init__(self):
        self.congestion_probability = 0.05  # 5% chance of congestion per sensor
        self.congestion_duration = {}  # Track ongoing congestion
        
    def get_time_multiplier(self):
        """Calculate traffic multiplier based on time of day"""
        current_hour = datetime.now().hour
        
        # Morning peak: 7-9 AM
        if 7 <= current_hour <= 9:
            return random.uniform(1.8, 2.5)
        # Evening peak: 5-7 PM
        elif 17 <= current_hour <= 19:
            return random.uniform(2.0, 2.8)
        # Lunch hour: 12-2 PM
        elif 12 <= current_hour <= 14:
            return random.uniform(1.3, 1.6)
        # Night: 10 PM - 5 AM
        elif current_hour >= 22 or current_hour <= 5:
            return random.uniform(0.2, 0.4)
        # Regular hours
        else:
            return random.uniform(0.8, 1.2)
    
    def should_trigger_congestion(self, sensor_id):
        """Determine if congestion should occur"""
        # Check if already in congestion
        if sensor_id in self.congestion_duration:
            self.congestion_duration[sensor_id] -= 1
            if self.congestion_duration[sensor_id] <= 0:
                del self.congestion_duration[sensor_id]
                logger.warning(f"🟢 CONGESTION CLEARED | {sensor_id}")
                return False
            return True
        
        # Random chance to trigger new congestion
        if random.random() < self.congestion_probability:
            # Congestion lasts 5-15 data points (minutes)
            duration = random.randint(5, 15)
            self.congestion_duration[sensor_id] = duration
            logger.warning(f"🔴 CONGESTION STARTED | {sensor_id} | Duration: {duration} readings")
            return True
        
        return False
    
    def generate_traffic_data(self, sensor):
        """Generate traffic data for a sensor"""
        time_multiplier = self.get_time_multiplier()
        is_congested = self.should_trigger_congestion(sensor['sensor_id'])
        
        # Calculate vehicle count
        base_count = sensor['base_vehicle_count']
        vehicle_count = int(base_count * time_multiplier * random.uniform(0.85, 1.15))
        
        # Calculate speed
        if is_congested:
            # Critical congestion: very low speed
            avg_speed = round(random.uniform(3, 9), 2)
            vehicle_count = int(vehicle_count * random.uniform(1.5, 2.0))  # More vehicles during congestion
        else:
            # Normal traffic variation
            base_speed = sensor['base_speed']
            speed_factor = 1.0 - (time_multiplier - 1.0) * 0.3  # Higher traffic = lower speed
            avg_speed = round(base_speed * speed_factor * random.uniform(0.85, 1.15), 2)
            avg_speed = max(10, min(avg_speed, 70))  # Clamp between 10-70 km/h
        
        return {
            'sensor_id': sensor['sensor_id'],
            'timestamp': datetime.now().isoformat(),
            'vehicle_count': vehicle_count,
            'avg_speed': avg_speed
        }


def create_kafka_producer():
    """Create and return Kafka producer instance"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"✅ Connected to Kafka broker: {KAFKA_BROKER}")
        return producer
    except KafkaError as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        raise


def send_traffic_data(producer, data):
    """Send traffic data to Kafka topic"""
    try:
        future = producer.send(KAFKA_TOPIC, value=data)
        record_metadata = future.get(timeout=10)
        
        # Log with traffic status indicator
        status_icon = "🚨" if data['avg_speed'] < 10 else "⚠️" if data['avg_speed'] < 20 else "✅"
        logger.info(
            f"{status_icon} SENT | "
            f"{data['sensor_id']} | "
            f"Vehicles: {data['vehicle_count']:3d} | "
            f"Speed: {data['avg_speed']:5.2f} km/h | "
            f"Partition: {record_metadata.partition}"
        )
        
    except KafkaError as e:
        logger.error(f"❌ Failed to send data: {e}")


def main():
    """Main producer loop"""
    logger.info("=" * 80)
    logger.info("SMART CITY TRAFFIC MONITORING SYSTEM - DATA PRODUCER")
    logger.info("=" * 80)
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Sensors: {len(SENSORS)}")
    logger.info(f"Data Frequency: 1 second per sensor")
    logger.info(f"Congestion Probability: 5% per reading")
    logger.info("=" * 80)
    
    # Create producer
    producer = create_kafka_producer()
    generator = TrafficDataGenerator()
    
    # Create Kafka topic with 4 partitions (one per sensor)
    logger.info("🔧 Kafka topic 'traffic-data' will be auto-created with default partitions")
    
    try:
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"\n--- Iteration {iteration} ---")
            
            # Generate and send data for each sensor
            for sensor in SENSORS:
                data = generator.generate_traffic_data(sensor)
                send_traffic_data(producer, data)
            
            # Wait 1 second before next batch
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Shutting down producer...")
    finally:
        producer.close()
        logger.info("✅ Producer closed successfully")


if __name__ == "__main__":
    main()
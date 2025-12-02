from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as _sum, count,
    current_timestamp, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, FloatType, TimestampType
)
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = "kafka:9093"
KAFKA_TOPIC = "traffic-data"
KAFKA_ALERT_TOPIC = "critical-traffic"
CHECKPOINT_DIR = "/opt/spark-jobs/checkpoints"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/traffic_db"
POSTGRES_PROPERTIES = {
    "user": "smartcity",
    "password": "smartcity123",
    "driver": "org.postgresql.Driver"
}

# Congestion detection threshold
CONGESTION_SPEED_THRESHOLD = 80.0  # km/h

# Define schema for incoming traffic data
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed", FloatType(), True)
])


def create_spark_session():
    spark = SparkSession.builder \
        .appName("SmartCityTrafficProcessing") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    return spark


def read_kafka_stream(spark):
    logger.info(f"Connecting to Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    logger.info("Connected to Kafka stream")
    return df


def parse_traffic_data(df):
    parsed_df = df.select(
        from_json(col("value").cast("string"), traffic_schema).alias("data")
    ).select("data.*")
    
    # Convert timestamp string to timestamp type
    parsed_df = parsed_df.withColumn(
        "timestamp",
        col("timestamp").cast(TimestampType())
    )
    
    logger.info("Traffic data schema parsed")
    return parsed_df


def calculate_congestion_index(windowed_df):
    congestion_df = windowed_df.withColumn(
        "congestion_index",
        when(col("avg_speed") > 0, (col("total_vehicles") / col("avg_speed")) * 100)
        .otherwise(lit(1000))  # Extremely high index for stopped traffic
    )
    
    # Classify severity
    congestion_df = congestion_df.withColumn(
        "severity",
        when(col("avg_speed") < 10, lit("CRITICAL"))
        .when(col("avg_speed") < 20, lit("HIGH"))
        .when(col("avg_speed") < 40, lit("MODERATE"))
        .otherwise(lit("NORMAL"))
    )
    
    return congestion_df


def process_with_windowing(traffic_df):
    logger.info("Applying 5-minute tumbling windows...")
    
    windowed_df = traffic_df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("sensor_id")
        ) \
        .agg(
            avg("vehicle_count").alias("avg_vehicle_count"),
            _sum("vehicle_count").alias("total_vehicles"),
            avg("avg_speed").alias("avg_speed"),
            count("*").alias("reading_count")
        ) \
        .select(
            col("sensor_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_vehicle_count"),
            col("total_vehicles"),
            col("avg_speed"),
            col("reading_count")
        )
    
    # Add congestion index
    windowed_df = calculate_congestion_index(windowed_df)
    
    return windowed_df


def detect_critical_traffic(windowed_df):
    critical_df = windowed_df.filter(col("avg_speed") < CONGESTION_SPEED_THRESHOLD)
    
    critical_df = critical_df.withColumn(
        "alert_timestamp", current_timestamp()
    )
    
    return critical_df


def write_to_postgres(table_name):
    def write_batch(batch_df, batch_id):
        logger.info(f"Writing batch {batch_id} to PostgreSQL table: {table_name} count: {batch_df.count()}")
        batch_df.write \
            .jdbc(
                url=POSTGRES_URL,
                table=table_name,
                mode='append',
                properties=POSTGRES_PROPERTIES
            )
        logger.info(f"Batch {batch_id} written successfully")
    
    return write_batch


def write_to_kafka(batch_df, batch_id):
    if batch_df.count() > 0:
        logger.warning(f"ALERT: Critical traffic detected! Writing {batch_df.count()} alerts to Kafka")

        # Convert to JSON for Kafka
        alert_df = batch_df.selectExpr(
            "sensor_id",
            "CAST(alert_timestamp AS STRING) as alert_timestamp",
            "CAST(window_start AS STRING) as window_start",
            "CAST(window_end AS STRING) as window_end",
            "CAST(avg_speed AS STRING) as avg_speed",
            "CAST(total_vehicles AS STRING) as total_vehicles",
            "CAST(congestion_index AS STRING) as congestion_index",
            "severity"
        )

        alert_df.selectExpr("to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", KAFKA_ALERT_TOPIC) \
            .save()

        logger.info(f"Alerts sent to Kafka topic: {KAFKA_ALERT_TOPIC}")
    else:
        logger.info(f"No critical alerts in this batch")



def console_output(df, query_name):
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName(query_name) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    return query


def main():
    logger.info("=" * 80)
    logger.info("SMART CITY TRAFFIC - SPARK STRUCTURED STREAMING PROCESSOR")
    logger.info("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read from Kafka
        raw_stream = read_kafka_stream(spark)
        
        # Parse traffic data
        traffic_stream = parse_traffic_data(raw_stream)
        
        # Write raw data to PostgreSQL
        logger.info("Starting raw data ingestion to PostgreSQL...")
        raw_query = traffic_stream.writeStream \
            .foreachBatch(write_to_postgres("traffic_events")) \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw") \
            .start()
        
        # Process with windowing
        windowed_stream = process_with_windowing(traffic_stream)
        
        # Detect critical traffic
        critical_stream = detect_critical_traffic(windowed_stream)
        
        # Write critical alerts to PostgreSQL
        logger.info("Starting critical traffic alert processing...")
        alert_query = critical_stream.writeStream \
            .foreachBatch(write_to_postgres(table_name="congestion_alerts")) \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/alerts") \
            .start()
        
        # Write alerts to Kafka topic
        kafka_alert_query = critical_stream.writeStream \
            .foreachBatch(write_to_kafka) \
            .outputMode("append") \
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka_alerts") \
            .start()
        
        # Console output for monitoring
        console_query = windowed_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        logger.info("=" * 80)
        logger.info("ALL STREAMING QUERIES STARTED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Window Size: 5 minutes (tumbling)")
        logger.info(f"Congestion Threshold: < {CONGESTION_SPEED_THRESHOLD} km/h")
        logger.info(f"Checkpoint Directory: {CHECKPOINT_DIR}")
        logger.info("=" * 80)
        
        # Wait for all queries
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"Error in streaming application: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
# Pseudo-code / Skeleton for Flink Anomaly Detection Job (PyFlink)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def run_anomaly_detection():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # 1. Define source (Kafka topic: otel-telemetry-metrics)
    t_env.execute_sql("""
        CREATE TABLE raw_metrics (
            service STRING,
            metric_name STRING,
            metric_value DOUBLE,
            ts TIMESTAMP(3),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'otel-metrics',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """)

    # 2. Continuous Feature Engineering (Rolling Z-Score)
    # Using a 5-minute sliding window to detect anomalies
    t_env.execute_sql("""
        CREATE VIEW metric_stats AS
        SELECT 
            service, 
            metric_name,
            AVG(metric_value) OVER w as avg_val,
            STDDEV(metric_value) OVER w as stddev_val,
            metric_value,
            ts
        FROM raw_metrics
        WINDOW w AS (PARTITION BY service, metric_name ORDER BY ts RANGE INTERVAL '5' MINUTE PRECEDING)
    """)

    # 3. Sink Structured Signals to Kafka
    t_env.execute_sql("""
        INSERT INTO KafkaSignals
        SELECT 
            UUID() as id,
            ts as timestamp,
            service,
            'metric_anomaly' as type,
            (metric_value - avg_val) / NULLIF(stddev_val, 0) as severity,
            'Detected metric anomaly' as description
        FROM metric_stats
        WHERE ABS((metric_value - avg_val) / NULLIF(stddev_val, 0)) > 3.0
    """)

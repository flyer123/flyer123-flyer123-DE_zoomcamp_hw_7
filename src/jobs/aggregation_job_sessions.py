from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_source(t_env):
    t_env.execute_sql("""
        CREATE TABLE events (
            kafka_key STRING,
            PULocationID INT,
            DOLocationID INT,
            trip_distance DOUBLE,
            lpep_pickup_datetime STRING,

            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'scan.startup.mode' = 'earliest-offset',

            'key.format' = 'json',
            'key.fields' = 'kafka_key',

            'value.format' = 'json'
        )
    """)


def create_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE processed_events_sessions (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,

            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'processed_events_sessions',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver',

            -- 🔧 Critical for stability
            'sink.buffer-flush.max-rows' = '200',
            'sink.buffer-flush.interval' = '2s',
            'sink.max-retries' = '5'
        )
    """)


def run_job():
    env = StreamExecutionEnvironment.get_execution_environment()

    # ✔ Checkpointing (required for JDBC consistency)
    env.enable_checkpointing(10000)

    # ✔ Keep simple unless partitions increased
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_source(t_env)
    create_sink(t_env)

    t_env.execute_sql("""
        INSERT INTO processed_events_sessions
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(
                TABLE events PARTITION BY PULocationID,
                DESCRIPTOR(event_timestamp),
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY window_start, window_end, PULocationID
    """).wait()


if __name__ == "__main__":
    run_job()
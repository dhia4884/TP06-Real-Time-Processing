CREATE TABLE checkins (
    department STRING,
    event_time STRING,

    event_time_ts AS TO_TIMESTAMP_LTZ(
        UNIX_TIMESTAMP(
            event_time,
            'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
        ) * 1000,
        3
    ),

    WATERMARK FOR event_time_ts AS
        event_time_ts - INTERVAL '5' SECOND
)
WITH (
    'connector' = 'kafka',
    'topic' = 'department-checkins',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-checkin-group-v3',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

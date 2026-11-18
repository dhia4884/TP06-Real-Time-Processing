SELECT
    window_start,
    window_end,
    department,
    COUNT(*) AS checkin_count
FROM TABLE(
    TUMBLE(
        TABLE checkins,
        DESCRIPTOR(event_time_ts),
        INTERVAL '1' MINUTE
    )
)
GROUP BY
    window_start,
    window_end,
    department;

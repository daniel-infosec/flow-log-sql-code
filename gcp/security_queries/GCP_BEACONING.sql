WITH helper AS (
    SELECT
        src_vm_name,
        SRC_IP,
        src_port,
        dst_ip,
        dst_port,
        packets,
        bytes,
        start_time,
        end_time,
        lag(start_time) OVER (
            PARTITION BY src_vm_name,
            dst_ip
            ORDER BY
                start_time desc
        ) AS prev_starttime,
        datediff(minute, start_time, end_time) AS connection_duration,
        datediff(minute, start_time, prev_starttime) AS prev_time_diff
    FROM
        gcp_flow_logs
    WHERE
        recorded_at >= dateadd(hour, -4, current_timestamp)
        AND SRC_IP LIKE '10.%'
        AND dst_ip NOT LIKE '10.%'
),
aggregator AS (
    SELECT
        count(*) AS beacon_count,
        avg(prev_time_diff) AS avg_prev_time_diff,
        stddev(prev_time_diff) AS stddev_prev_time_diff,
        variance(prev_time_diff) AS variance_prev_time_diff,
        avg(connection_duration) AS avg_connection_duration,
        stddev(connection_duration) AS stddev_connection_duration,
        src_vm_name,
        src_ip,
        dst_ip
    FROM
        helper
    WHERE
        prev_starttime IS NOT NULL
    GROUP BY
        7,
        8,
        9
)
SELECT
    *
FROM
    aggregator
WHERE
    beacon_count > 100
    AND VARIANCE_PREV_TIME_DIFF < 0.3
    AND avg_prev_time_diff > 1

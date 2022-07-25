WITH last_hour_misses AS (
    SELECT
        count(a.*) AS miss_count,
        src_vm_name,
        dst_tags ['instance_role'] AS dst_role,
        src_tags ['instance_role'] AS src_role
    FROM
        gcp_flow_logs_join_vms a
    where
        recorded_at >= dateadd(hour, -1, current_timestamp)
        AND start_time = end_time
        AND bytes = 0
    GROUP BY
        2,
        3,
        4
)
SELECT
    *
FROM
    last_hour_misses a
    INNER JOIN gcp_connect_baseline ON a.dst_role = baseline.dst_role
    AND a.src_role = baseline.src_role
WHERE
    miss_count > avg_miss_count + stddev_miss_count

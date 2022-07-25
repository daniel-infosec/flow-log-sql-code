-- In my testing, scanning in GCP flow logs shows up AS a connection with 0 bytes sent
-- WHERE the start AND end time are the same. This is due to GCP not surfacing an reject/deny
-- like AWS AND Azure do
-- Get average count of instance type by role
WITH counter AS (
    SELECT
        count(a.*) AS miss_count,
        dst_tags ['instance_role'] AS dst_role,
        src_tags ['instance_role'] AS src_role,
        date_part(hour, recorded_at) AS hourly_part
    FROM
        gcp_flow_logs_join_vms
    WHERE
        recorded_at BETWEEN dateadd(hour, -25, current_timestamp)
            AND dateadd(hour, -1, current_timestamp)
        AND start_time = end_time
        AND bytes = 0
    GROUP BY
        2,
        3,
        4
),
instance_count AS (
    SELECT
        count(*) AS daily_instance_count,
        tags ['instance_role'] AS count_instance_role
    FROM
        gcp_vms
    WHERE
        last_seen BETWEEN dateadd(hour, -25, current_timestamp)
            AND dateadd(hour, -1, current_timestamp)
    GROUP BY
        2
)
SELECT
    avg(miss_count) AS avg_miss_count,
    stddev(miss_count) AS stddev_miss_count,
    src_role,
    dst_role
FROM
    counter
INNER JOIN instance_count ON src_role = count_instance_role
GROUP BY
    3,
    4

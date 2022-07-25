WITH rst_by_role_last_hour AS (
    SELECT
        count(*) AS rst_last_hour,
        instanceid,
        tags ['instance_role'] AS instance_role
    FROM
        aws_flow_logs a
        LEFT OUTER JOIN aws_instances b ON a.instanceid = b.instance_id
    WHERE
        a.recorded_at >= dateadd(hour, -1, current_timestamp)
        -- Look for RST packets
        AND TCPFLAGS = 4
    GROUP BY
        2,
        3
)
SELECT
    *
FROM
    rst_by_role_last_hour a
    INNER JOIN aws_rst_baseline b ON a.instance_role = b.instance_role
WHERE
    -- In this example, we'll look for spikes that are 1 standard deviation above for the last hour
    rst_last_hour >= avg_rst_by_role + stddev_rst_by_role

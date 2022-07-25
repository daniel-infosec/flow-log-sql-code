-- Get average count of instance type by role
WITH count_by_role AS (
    SELECT
        count(*) AS role_count,
        tags ['instance_role'] AS role_count_tag
    FROM
        aws_instances
    WHERE
        latest_available_data BETWEEN dateadd(hour, -25, current_timestamp)
            AND dateadd(hour, -1, current_timestamp)
    GROUP BY
        2
),
-- Get number of rsts by role per hour
aggregator AS (
    SELECT
        count(*) AS total_rst_count,
        tags ['instance_role'] AS instance_role,
        date_part(hour, a.recorded_at) AS hourly_part
    FROM
        aws_flow_logs a
        LEFT OUTER JOIN aws_instances b ON a.instanceid = b.instance_id
        INNER JOIN count_by_role ON tags ['instance_role'] = role_count_tag
    WHERE
        a.recorded_at BETWEEN dateadd(hour, -25, current_timestamp)
            AND dateadd(hour, -1, current_timestamp)
        AND TCPFLAGS = 4
    GROUP BY
        2,
        3
),
-- Get # of rsts by role per instance
rate_calculator AS (
    SELECT
        instance_role,
        total_rst_count / role_count AS rate,
        hourly_part
    FROM
        joiner
)
-- Get avg and standard deviation
SELECT
    avg(rate) AS avg_rst_by_role,
    stddev(rate) AS stddev_rst_by_role,
    instance_role
FROM
    rate_calculator
GROUP BY
    3

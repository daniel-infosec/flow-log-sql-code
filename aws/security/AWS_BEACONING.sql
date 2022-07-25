-- Basic beaconing detection
WITH helper AS (
    SELECT
        accountid,
        instanceid,
        srcaddr,
        srcport,
        dstaddr,
        dstport,
        packets,
        bytes,
        starttime,
        endtime,
        -- Get difference in time between previous connection and connection duration
        lag(starttime) OVER (
            PARTITION BY instanceid,
            dstaddr
            ORDER BY
                starttime DESC
        ) AS prev_starttime,
        datediff(minute, starttime, endtime) AS connection_duration,
        datediff(minute, starttime, prev_starttime) AS prev_time_diff
    FROM
        aws_flow_logs
    WHERE
        -- Get last 4 hours of data and filter on internal IPs (assuming 10.0.0.0/8 addressing schema)
        recorded_at >= dateadd(hour, -4, current_timestamp)
        AND srcaddr like '10.%'
        AND dstaddr not like '10.%'
        AND tcpflags in (2, 3)
        AND action = 'ACCEPT'
),
aggregator AS (
    SELECT
        count(*) AS beacon_count,
        avg(prev_time_diff) AS avg_prev_time_diff,
        stddev(prev_time_diff) AS stddev_prev_time_diff,
        variance(prev_time_diff) AS variance_prev_time_diff,
        avg(connection_duration) AS avg_connection_duration,
        stddev(connection_duration) AS stddev_connection_duration,
        instanceid,
        srcaddr,
        dstaddr
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
    -- Super basic beaconing detection where we look for 100 beacons and a low variance
    beacon_count > 100
    AND variance_prev_time_diff < 0.3
    -- filter on beacons more than once a minute
    AND avg_prev_time_diff > 1

WITH counter AS (
    SELECT
        count(*) AS role_denied_count,
        src_vm_tags ['instance_role'] AS src_role,
        date_trunc(hour, recorded_at)
    FROM
        azure_flow_logs_join_vm_nics
    WHERE
        recorded_at between dateadd(hour, -25, current_timestamp)
        AND dateadd(hour, -1, current_timestamp)
        AND TRAFFIC_DECISION = 'DENIED'
    GROUP BY
        2,
        3
),
instance_role_count AS (
    SELECT
        count(*) AS role_count,
        vm_tags ['instance_role'] AS role_count_role
    FROM
        azure_vms
    WHERE
        window_end BETWEEN dateadd(hour, -25, current_timestamp)
            AND dateadd(hour, -1, current_timestamp)
    GROUP BY
        2
)
SELECT
    avg(role_denied_count / role_count) AS avg_denied_count,
    stddev(role_denied_count / role_count) AS stddev_denied_count,
    src_role
FROM
    counter
    LEFT OUTER JOIN instance_role_count ON role_count_role = src_role
GROUP BY
    3

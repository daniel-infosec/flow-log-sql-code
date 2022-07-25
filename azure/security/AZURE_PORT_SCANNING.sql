WITH denied_count AS (
    SELECT
        count(*) AS role_denied_count,
        src_vm_id,
        src_vm_tags ['instance_role'] AS src_role,
        date_trunc(hour, recorded_at)
    FROM
        azure_flow_logs_join_vm_nics
    WHERE
        recorded_at >= dateadd(hour, -1, current_timestamp)
        AND traffic_decision = 'DENIED'
    GROUP BY
        2,
        3,
        4
)
SELECT
    src_vm_id, role_denied_count, denied_count.src_role
FROM
    denied_count
INNER JOIN azure_denied_baseline ON denied_count.src_role = azure_denied_baseline.src_role
WHERE
    role_denied_count > avg_denied_count + stddev_denied_count

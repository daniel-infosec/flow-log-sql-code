-- Start by pulling out VM creation events from audit logs
-- https://docs.microsoft.com/en-us/azure/virtual-machines/monitor-vm-reference
-- https://docs.microsoft.com/en-us/azure/azure-monitor/essentials/activity-log-schema
WITH vm_create_events AS (
    SELECT
        parse_json(properties['responseBody'])['id'] AS vm_id,
        parse_json(properties['responseBody']) AS vm_details,
        *
    FROM
        azure_logs
    WHERE
        operation_name = 'MICROSOFT.COMPUTE/VIRTUALMACHINES/WRITE'
        AND category = 'Write'
        AND RESULT_SIGNATURE = 'Accepted.Created'
),
-- Union VM creation log data with VM inventory data
vm_union AS (
    SELECT
        min(recorded_at) AS earliest,
        max(recorded_at) AS latest,
        tags,
        tenant_id,
        subscription_id,
        id AS vm_id,
        location,
        name AS vm_name,
        properties
    FROM
        -- sourced FROM https://github.com/snowflakedb/SnowAlert/blob/master/src/connectors/azure_collect.py
        -- I'm sure you can modify this to use something like https://github.com/cloudquery/cloudquery
        azure_collect_virtual_machines
    WHERE
        -- Only pull latest 30 days for performance
        recorded_at >= dateadd(day, -30, current_timestamp())
    group BY
        3,4,5,6,7,8,9
    UNION
    SELECT
        min(recorded_at) AS earliest,
        max(recorded_at) AS latest,
        vm_details['tags'] AS tags,
        raw['tenantId']::STRING AS tenant_id,
        split(vm_id, '/')[2]::STRING AS subscription_id,
        vm_id::STRING AS vm_id,
        vm_details['location'] :: STRING AS location,
        vm_details['properties']['osProfile']['computerName']::STRING AS vm_name,
        vm_details['properties'] AS properties
    FROM
        vm_create_events
    WHERE
        -- Only pull latest 30 days for performance
        recorded_at >= dateadd(day, -30, current_timestamp())
    group BY
        3,4,5,6,7,8,9
) ,
-- Get earliest seen time
true_earliest_vms AS (
    SELECT
        a.*,
        window_start
    FROM
        vm_union a
        join (
            SELECT
                vm_id,
                min(earliest) AS window_start
            FROM
                vm_union
            GROUP BY
                vm_id
        ) b ON a.vm_id = b.vm_id
),
-- Get termination events
term AS (
    SELECT
        *
    FROM
        azure_logs
    WHERE
        operation_name = 'MICROSOFT.COMPUTE/VIRTUALMACHINES/DELETE'
        AND category = 'Delete'
        AND RESULT_SIGNATURE = 'Succeeded.'
        AND RESULT_TYPE = 'Success'
        -- Only pull the latest 30 days of data for performance
        AND recorded_at >= dateadd(day, -30, current_timestamp())
)
-- Join termination events to get window start and window end
SELECT
    DISTINCT
    window_start,
    coalesce(term.event_time, latest) AS window_end,
    tenant_id,
    subscription_id,
    vm_id,
    vm_name,
    true_earliest_vms.properties,
    true_earliest_vms.location
FROM
    true_earliest_vms
    LEFT OUTER JOIN term ON lower(vm_id) = lower(resource_id)

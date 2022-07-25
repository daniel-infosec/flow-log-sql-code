-- First, collect VM creation from cloud audit logs
WITH gcp_instance_create_raw AS (
    SELECT
        account,
        recorded_at,
        raw,
        raw['protoPayload']['request']['name']::STRING AS vm_name,
        raw['resource']['labels']['project_id']::STRING AS project_name,
        raw['resource']['labels']['zone']::STRING AS zone,
        raw['protoPayload']['resourceName']::STRING AS resource_name,
        raw['protoPayload']['request']['disks'] AS disks,
        FLATTEN_OBJECTS_ARRAY(
            raw['protoPayload']['request']['labels'],
            'key',
            'value'
        ) AS tags,
        raw['protoPayload']['request']['networkInterfaces'] AS nics,
        CONVERT_TIMEZONE(
            'UTC',
            raw['protoPayload']['response']['startTime']::TIMESTAMP_TZ
        )::TIMESTAMP_LTZ AS start_time
    FROM
        gcp_audit_logs,
        lateral flatten(raw['protoPayload']['request']['labels'])
    where
        log_type = 'cloudaudit.googleapis.com/activity'
        and raw['resource']['type'] = 'gce_instance'
        and raw['protoPayload']['response']['operationType'] = 'insert'
        and recorded_at >= dateadd(day, -30, current_timestamp())
),
-- Group to get earliest seen, latest seen, and other relevant data and remove duplicates
gcp_instance_create AS (
    SELECT
        min(least(recorded_at, start_time)) AS earliest,
        max(recorded_at) AS latest,
        account,
        vm_name,
        project_Name,
        zone,
        resource_name,
        tags,
        -- VM creation doesn't tell us the IPs in use unfortunatly
        NULL AS nics,
        start_time
    FROM
        gcp_instance_create_raw
    GROUP BY
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
-- Using cloud asset inventory
-- https://cloud.google.com/asset-inventory/docs/overview
-- Assuming the JSON object is in a column called "raw"
gcp_snapshot_raw AS (
    SELECT
        account,
        recorded_at,
        split_part(raw['name'], '/', -1)::STRING AS vm_name,
        split_part(raw['name'], '/', 5)::STRING AS project_name,
        raw['resource']['location']::STRING AS zone,
        substr(raw['name'], 26)::STRING AS resource_name,
        raw['resource']['data']['disks'] AS disks,
        raw['resource']['data']['labels'] AS tags,
        raw['resource']['data']['networkInterfaces'] AS nics,
        CONVERT_TIMEZONE(
            'UTC',
            raw['resource']['data']['creationTimestamp']::TIMESTAMP_TZ
        )::TIMESTAMP_LTZ AS start_time
    FROM
        gcp_cloud_assets
    where
        log_type = 'cloudassets'
        and raw['asset_type'] = 'compute.googleapis.com/Instance'
        and recorded_at >= dateadd(day, -3, current_timestamp())
),
-- Group to remove duplicates
gcp_snapshot AS (
    SELECT
        min(recorded_at) AS earliest,
        max(recorded_at) AS latest,
        account,
        vm_name,
        project_Name,
        zone,
        resource_name,
        tags,
        nics,
        start_time
    FROM
        gcp_snapshot_raw
    GROUP BY
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
-- Union snapshot data and audit log data
all_instances AS (
    SELECT
        *,
        'snapshot' AS source
    FROM
        gcp_snapshot
    UNION
    ALL
    SELECT
        *,
        'create' AS source
    FROM
        gcp_instance_create
),
-- Group to get latest and earliest seen 
final_instances_helper AS (
    SELECT
        min(a.start_time) AS start_time,
        TRUE_EARLIEST,
        TRUE_LATEST,
        min(A.account) AS account,
        min(a.vm_name) AS vm_name,
        min(a.project_name) AS project_name,
        min(a.zone) AS zone,
        min(a.resource_name) AS resource_name,
        min(a.tags) AS tags,
        a.nics
    FROM
        all_instances A
        JOIN (
            SELECT
                resource_name,
                MAX(latest) AS true_latest,
                MIN(LEAST(earliest, start_time)) AS true_earliest
            FROM
                all_instances
            GROUP BY
                resource_name
        ) B ON A.resource_name = B.resource_name
        AND A.latest = B.true_latest
    GROUP BY
        2,
        3,
        10
),
-- Removes NULL nics and gets earliest start time
final_instances_helper_two AS (
    SELECT
        min(a.start_time) AS start_time,
        a.true_earliest,
        a.true_latest,
        a.account,
        a.vm_name,
        a.project_name,
        a.zone,
        a.resource_name,
        a.tags,
        max(nics) AS nics
    FROM
        final_instances_helper a
    GROUP BY
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
),
-- Get VM termination events
term AS (
    SELECT
        account,
        recorded_at,
        raw['resource']['labels']['project_id']::STRING AS project_name,
        raw['resource']['labels']['zone']::STRING AS zone,
        raw['protoPayload']['resourceName']::STRING AS resource_name,
        CONVERT_TIMEZONE('UTC', raw['receiveTimestamp']::TIMESTAMP_TZ)::TIMESTAMP_LTZ AS term_time
    FROM
        gcp_audit_logs
    where
        log_type = 'cloudaudit.googleapis.com/activity'
        and raw['resource']['type'] = 'gce_instance'
        and raw['protoPayload']['methodName'] = 'v1.compute.instances.delete'
        and raw['protoPayload']['response']['operationType'] = 'delete'
        and recorded_at >= dateadd(day, -30, current_timestamp())
)
-- Join on termination events and set last seen based on either recorded at or term time
SELECT
    a.account,
    a.vm_name,
    a.project_name,
    a.zone,
    a.resource_name,
    a.tags,
    a.nics,
    least(a.start_time, a.true_earliest) AS earliest,
    greatest(
        a.true_latest,
        COALESCE(b.term_time, a.true_latest)
    ) AS last_seen,
    b.term_time
FROM
    final_instances_helper_two a
    left outer join TERM b on a.resource_name = b.resource_name

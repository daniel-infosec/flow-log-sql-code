-- Reference
-- https://cloud.google.com/vpc/docs/flow-logs#record_format
-- I recommend you parse this out on ingest for performance reasons, but if you don't, then here you go
SELECT
    recorded_at,
    raw ['timestamp']::timestamp AS event_time,
    raw ['jsonPayload'] ['start_time']::timestamp AS start_time,
    raw ['jsonPayload'] ['end_time']::timestamp AS end_time,
    id AS log_id,
    deployment,
    account,
    raw ['insertId']::varchar AS insert_id,
    raw ['jsonPayload'] ['connection'] ['dest_ip']::varchar AS dst_ip,
    raw ['jsonPayload'] ['connection'] ['dest_port'] AS dst_port,
    raw ['jsonPayload'] ['connection'] ['src_ip']::varchar AS src_ip,
    raw ['jsonPayload'] ['connection'] ['src_port']::int AS src_port,
    raw ['jsonPayload'] ['connection'] ['protocol']::int AS protocol,
    raw ['jsonPayload'] ['bytes_sent']::int AS bytes,
    raw ['jsonPayload'] ['packets_sent']::int AS packets,
    raw ['jsonPayload'] ['dest_instance'] ['project_id']::varchar AS dst_project,
    raw ['jsonPayload'] ['dest_instance'] ['region']::varchar AS dst_region,
    raw ['jsonPayload'] ['dest_instance'] ['vm_name']::varchar AS dst_vm_name,
    raw ['jsonPayload'] ['dest_instance'] ['zone']::varchar AS dst_zone,
    raw ['jsonPayload'] ['dest_vpc'] ['subnetwork_name']::varchar AS dst_subnet_name,
    raw ['jsonPayload'] ['src_instance'] ['project_id']::varchar AS src_project,
    raw ['jsonPayload'] ['src_instance'] ['region']::varchar AS src_region,
    raw ['jsonPayload'] ['src_instance'] ['vm_name']::varchar AS src_vm_name,
    raw ['jsonPayload'] ['src_instance'] ['zone']::varchar AS src_zone,
    raw ['jsonPayload'] ['src_vpc'] ['subnetwork_name']::varchar AS src_subnet_name,
    raw ['jsonPayload'] ['reporter']::varchar AS reporter,
    raw
FROM
    gcp_audit_logs
WHERE
    log_type = 'compute.googleapis.com/vpc_flows'
AND
    -- Only pull the last 1 hour for performance
    recorded_at >= dateadd(hour, -1, current_timestamp)

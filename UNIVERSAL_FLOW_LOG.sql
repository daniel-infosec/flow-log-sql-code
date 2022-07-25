-- This is meant to show how few fields all 3 of these providers have in common
SELECT
    recorded_at,
    start AS event_time,
    srcaddr AS src_addr,
    srcport AS src_port,
    dstaddr AS dst_addr,
    dstport AS dst_port,
    CASE WHEN protocol = 6 THEN 'TCP'
         WHEN protocol = 17 THEN 'UDP'
    END AS protocol
FROM aws_vpc_flow_logs
    -- only capture TCP/UDP traffic
    WHERE protocol in (6,17)
UNION ALL
-- We do make some assumptions on GCP flow logs with regards to source/destination since, as said in the presentation
-- we can't definitively establish direction
SELECT
    recorded_at,
    start_time as event_time,
    src_ip as src_addr,
    src_port,
    dst_ip as dst_addr,
    dst_port,
    CASE WHEN protocol = 6 THEN 'TCP'
         WHEN protocol = 17 THEN 'UDP'
    END AS protocol
FROM gcp_flow_logs
WHERE protocol in (6,17)
UNION ALL
SELECT
    recorded_at,
    -- you could look to join azure flow logs on themselves to match up begin and end of traffic flow
    -- but that would be horrible for performance
    event_time,
    src_addr,
    src_port,
    dst_addr,
    dst_port,
    protocol
FROM azure_flow_logs

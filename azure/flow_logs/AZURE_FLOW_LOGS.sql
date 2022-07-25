-- I highly recommend you transform the Azure flow logs on ingest otherwise your performance will suffer
-- But if you didn't, here's some SQL to help you out
WITH flow_parser AS (
    SELECT
        recorded_at,
        flow_raw ['category']::varchar AS category,
        flow_raw ['macAddress']::varchar AS mac_address,
        flow_raw ['operationName']::varchar AS operation_name,
        flow_raw ['properties'] AS properties,
        flow_raw ['resourceId']::varchar AS resource_id,
        flow_raw ['systemId']::varchar AS system_id,
        flow_raw ['time']::timestamp AS time_stamp,
        value AS flow_raw
    FROM
        azure_flow_logs_raw,
        lateral flatten(flow_raw['properties']['flows'])
)
, rule_parser AS (
    SELECT
        recorded_at,
        category,
        mac_address,
        operation_name,
        properties,
        resource_id,
        system_id,
        event_Time,
        flow_raw,
        flow_raw ['rule'] AS rule,
        value AS flow_tuples
    FROM
        helper,
        lateral flatten(flow_raw ['flows'])
)
SELECT
    recorded_at,
    category,
    mac_address,
    operation_name,
    properties,
    resource_id,
    system_id,
    time_stamp,
    flow_raw,
    rule,
    flow_tuples,
    flow_tuples['mac']::varchar AS mac_address,
    value AS raw_flow,
    to_timestamp(split(raw_flow[','])[0]) AS event_time,
    split(raw_flow[','])[1]::varchar AS src_addr,
    split(raw_flow[','])[2]::varchar AS dst_addr,
    split(raw_flow[','])[3]::int AS src_port,
    split(raw_flow[','])[4]::int AS dst_addr,
    CASE
        WHEN trim(split(raw_flow[','])[5]) = 't' THEN 'TCP'
        WHEN trim(split(raw_flow[','])[5]) = 'u' THEN 'UDP'
    END AS protocol,
    CASE
        WHEN trim(split(raw_flow[','])[6]) = 'i' THEN 'INBOUND'
        WHEN trim(split(raw_flow[','])[6]) = 'o' THEN 'OUTBOUND'
    END AS traffic_flow,
    CASE
        WHEN trim(split(raw_flow[','])[7]) = 'a' THEN 'ALLOWED'
        WHEN trim(split(raw_flow[','])[7]) = 'd' THEN 'DENIED'
    END AS traffic_decision,
    CASE
        WHEN trim(split(raw_flow[','])[8]) = 'b' THEN 'BEGIN'
        WHEN trim(split(raw_flow[','])[8]) = 'c' THEN 'CONTINUING'
        WHEN trim(split(raw_flow[','])[8]) = 'e' THEN 'END'
    END AS flow_state,
    split(raw_flow[','])[9]::int AS packets_src_to_dst,
    split(raw_flow[','])[10]::int AS bytes_src_to_dst,
    split(raw_flow[','])[11]::int AS packets_dst_to_src,
    split(raw_flow[','])[12]::int AS bytes_dst_to_src,
FROM rule_parser,
lateral flatten(flow_raw ['flow_tuples'])

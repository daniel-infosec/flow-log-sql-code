-- Assuming we have a table of IOCs where ioc_value is an IP address
-- Compared to AWS, we have a lot less to filter on because we lack clarity on direction
-- and if a connection was accepted/rejected. :angryparrot:
SELECT
    *
FROM
    gcp_flow_logs_join_vms
    INNER JOIN iocs ON srcaddr = iocs.ioc_value or dstaddr = iocs.ioc_value

SELECT
    *
FROM
    azure_flow_logs_join_vm_nics
    INNER JOIN iocs ON (
        src_addr = iocs.ioc_value
        AND traffic_decision = 'ALLOWED'
    )
    OR (dst_addr = iocs.ioc_value)

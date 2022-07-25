-- Assuming we have a table of IOCs where ioc_value is an IP address
SELECT
    *
FROM
    aws_flow_logs_join_ec2
    INNER JOIN iocs ON 
    -- If the source address was an IOC and we accepted the connection, alert
    (
        srcaddr = iocs.ioc_value
        AND action = 'ACCEPT'
        -- If it's something connecting to our webserver on port 443, we'll ignore it
        AND NOT (
            flags['instance_role'] = 'apache'
            AND dstport = 443
        )
    )
    -- Or if we reached out to an IOC IP
    OR
    (
        dstaddr = iocs.ioc_value
    )
WHERE tcpflags IN (2,3)

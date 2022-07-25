SELECT
    *
FROM
    aws_flow_logs
    LEFT OUTER JOIN aws_enis a on interface_id = eni_id
    LEFT OUTER JOIN aws_enis b on CASE
        WHEN a.eni_id != interface_id
            AND srcaddr = a.private_ip
            AND dstaddr = b.private_ip
            AND start_time between b.window_start
              AND b.window_end
            THEN 1
        WHEN a.eni_id != interface_id
            AND dstaddr = a.private_ip
            AND srcaddr = b.private_ip
            AND start_time between b.window_start
              AND b.window_end
            THEN 1
        ELSE 0
    END = 1

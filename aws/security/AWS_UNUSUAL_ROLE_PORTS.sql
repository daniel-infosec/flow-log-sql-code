-- Pretty basic example assuming we have a jumpbox and apache web server
SELECT * FROM aws_flow_logs_join_ec2
WHERE
-- look for connections being initiated
tcpflags in (2,3)
-- assumign we're using instance_role to describe what the instance is for
AND
(
    -- Apache shouldn't be initiating connections to other systems
    src_tags['instance_role'] = 'apache'
    OR
    -- Our jump box should only be connecting to other instances on port 22
    (
        src_tags['instance_role'] = 'jump_box'
        and dstport != 22
    )
    OR
    -- Look for successful connections to our apache server from external sites that's not to port 443
    (
        dst_tags['instance_role'] = 'apache'
        AND action = 'ACCEPT'
        AND dstport != 443
        AND srcaddr NOT LIKE '10.%'
    )
    OR
    -- Look for internal hosts that don't have properly applied instance tags
    (
        srcaddr like '10.%'
        and
        (
            srctags IS NULL
            OR
            srctags NOT IN ('apache', 'jump_box')
        )
    )

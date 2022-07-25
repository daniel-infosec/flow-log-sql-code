-- Select from our flow logs and join on our view of VMs / NICs. We ensure that we join on not only the IP but
-- that the timestamp of the flow aligns with the IP of the VM 
SELECT
    DISTINCT a.recorded_at AS flow_recorded_at,
    a.time_stamp,
    a.event_time,
    a.mac_address,
    a.resource_id,
    a.rule,
    a.src_addr,
    a.dst_addr,
    a.src_port,
    a.dst_port,
    a.protocol,
    a.traffic_flow,
    a.traffic_decision,
    a.flow_state,
    a.PACKETS_SRC_TO_DST,
    a.BYTES_SRC_TO_DST,
    a.PACKETS_DST_TO_SRC,
    a.BYTES_DST_TO_SRC,
    b.nic_id AS src_nic_id,
    b.nic_name AS src_nic_name,
    b.nic_location AS src_nic_location,
    b.nic_properties AS src_nic_properties,
    b.nic_tags AS src_nic_tags,
    b.ip_configuration AS src_ip_configuration,
    b.ip_configuration [0] ['properties'] ['privateIPAddress'] as src_ip_addr,
    b.vm_tags AS src_vm_tags,
    b.vm_id AS src_vm_id,
    b.vm_name AS src_vm_name,
    b.vm_properties AS src_vm_properties,
    c.nic_id AS dst_nic_id,
    c.nic_name AS dst_nic_name,
    c.nic_location AS dst_nic_location,
    c.nic_properties AS dst_nic_properties,
    c.nic_tags AS dst_nic_tags,
    c.ip_configuration AS dst_ip_configuration,
    c.ip_configuration [0] ['properties'] ['privateIPAddress'] as dst_ip_addr,
    c.vm_tags AS dst_vm_tags,
    c.vm_id AS dst_vm_id,
    c.vm_name AS dst_vm_name,
    c.vm_properties AS dst_vm_properties
FROM
    azure_flow_logs a
    -- A nic can have multiple IP configurations, but for simplicity, we'll avoid the lateral flatten and assume our environment does not
    LEFT OUTER JOIN azure_vms_nics b ON src_addr = b.ip_configuration [0] ['properties'] ['privateIPAddress']
    AND a.event_time >= b.window_start
    AND (
        a.event_time <= b.window_end
                 b.window_end is NULL
    )
    AND LOWER(split(a.resource_id, '/') [2]) = b.subscription_id
    LEFT OUTER JOIN azure_vms_nics c ON dst_addr = c.ip_configuration [0] ['properties'] ['privateIPAddress']
    AND a.event_time >= c.window_start
    AND (
        a.event_time <= c.window_end
        OR c.window_end is NULL
    )

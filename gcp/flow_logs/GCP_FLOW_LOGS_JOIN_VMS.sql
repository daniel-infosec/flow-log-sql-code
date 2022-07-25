-- Our most basic join!
SELECT a.*,
    b.tags as src_tags,
    b.nics as src_nics,
    c.tags as dst_tags,
    c.nics as src_nics
    FROM gcp_flow_logs a
LEFT OUTER JOIN gcp_vms b on gcp_flow_logs.src_vm_name = b.vm_name
LEFT OUTER JOIN gcp_vms c on gcp_flow_logs.dst_vm_name = c.vm_name

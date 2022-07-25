WITH azure_vm_helper AS (
    SELECT
        window_start AS vm_window_start,
        window_end AS vm_window_end,
        tags AS vm_tags,
        tenant_id AS vm_tenant_id,
        subscription_id AS vm_subscription_id,
        vm_id,
        location AS vm_location,
        vm_name,
        properties AS vm_properties,
        LOWER(value ['id'])::VARCHAR AS vm_nic_id
    FROM
        azure_vms,
        -- A VM may have multiple NICs, so we'll have 1 row per NIC
        lateral flatten(properties ['networkProfile'] ['networkInterfaces'])
)
-- Join VMs on NICs. This will get us VM ID, NIC ID, and IP Configuration in one view
SELECT
    *
FROM
    azure_vm_helper  a
    INNER JOIN azure_nics b ON LOWER(a.vm_nic_id) = LOWER(b.nic_id)

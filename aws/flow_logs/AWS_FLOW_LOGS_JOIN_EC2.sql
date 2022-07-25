SELECT
  a.*,
  b.instance_id as src_instance_id,
  b.account_id as src_account_id,
  b.tags as src_tags,
  b.details as src_details,
  c.instance_id as dst_instance_id,
  c.account_id as dst_account_id,
  c.tags as dst_tags,
  c.details as dst_details
FROM
  aws_flow_logs a
  LEFT OUTER JOIN aws_instances b on a.instance_id = b.instance_id
  LEFT OUTER JOIN aws_instances c on CASE
    WHEN c.instance_id != a.interface_id
      AND srcaddr = c.IP_ADDRESS
      AND dstaddr = c.IP_ADDRESS
      AND start_time between c.window_start
        AND c.window_end
      THEN 1
    WHEN c.instance_id != a.interface_id
      AND dstaddr = c.ip_address
      AND srcaddr = c.ip_address
      AND start_time between c.window_start
        AND c.window_end 
      THEN 1
    ELSE 0
  END = 1

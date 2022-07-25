with elb_network_interface_cloudtrail as (
  select
    event_time as recorded_at,
    RECIPIENT_ACCOUNT_ID as account_id,
    RESPONSE_ELEMENTS ['networkInterface'] ['networkInterfaceId']::varchar as eni_id,
    RESPONSE_ELEMENTS ['privateIpAddress'] as private_ip,
    -- This is my best guess at estimating what object type is attached to this ENI
    CASE
      WHEN SOURCE_IP_ADDRESS = 'elasticloadbalancing.amazonaws.com' THEN 'network_load_balancer'
      WHEN SOURCE_IP_ADDRESS = 'ecs.amazonaws.com' THEN 'ecs'
      WHEN REQUEST_PARAMETERS ['description'] like 'aws-K8S-%' then 'eks'
      WHEN USER_IDENTITY_INVOKEDBY = 'elasticfilesystem.amazonaws.com' then 'efs'
      WHEN USER_IDENTITY_INVOKEDBY = 'eks.amazonaws.com' then 'eks'
      WHEN USER_IDENTITY_INVOKEDBY = 'rds.amazonaws.com' then 'rds'
      WHEN USER_IDENTITY_INVOKEDBY = 'clientvpn.amazonaws.com' then 'clientvpn'
      WHEN USER_IDENTITY_INVOKEDBY = 'elasticmapreduce.amazonaws.com' then 'emr'
      WHEN USER_IDENTITY_INVOKEDBY = 'transitgateway.amazonaws.com' then 'transitgateway'
      WHEN USER_IDENTITY_INVOKEDBY = 'redshift.amazonaws.com' then 'redshift'
      WHEN USER_IDENTITY_INVOKEDBY = 'es.amazonaws.com' then 'elasticsearch'
      WHEN SOURCE_IP_ADDRESS = 'apigateway.amazonaws.com' then 'apigateway'
      WHEN USER_IDENTITY_PRINCIPAL_ID like '%:AmazonSageMaker' then 'sagemaker'
      WHEN REQUEST_PARAMETERS ['description'] like 'AWS Lambda VPC ENI-%' then 'lambda'
      WHEN RESPONSE_ELEMENTS ['instanceId'] is null then 'ec2-instance'
      ELSE 'unknown'
    END as object_type,
    CASE
      WHEN object_type = 'network_load_balancer' THEN REQUEST_PARAMETERS ['description']::varchar
      ELSE 'unknown'
    END as object_name,
    RESPONSE_ELEMENTS ['instanceId'] as eni_instance_id,
    'network_interface_cloudtrail' as source
  from
    CLOUDTRAIL
  where
    event_name = 'CreateNetworkInterface'
    and ERROR_CODE is null
),
run_instances_enis as (
  select
    event_time as recorded_at,
    account_id,
    instances ['instanceId'] as eni_instance_id,
    value ['networkInterfaceId'] as eni_id,
    value ['privateIpAddress'] as private_ip
  from
    (
      select
        event_time,
        RECIPIENT_ACCOUNT_ID as account_id,
        value as instances
      from
        CLOUDTRAIL
        lateral flatten(
          input => RESPONSE_ELEMENTS ['instancesSet'] ['items']
        )
      where
        event_name = 'RunInstances'
    ),
    lateral flatten(
      input => instances ['networkInterfaceSet'] ['items']
    )
),
union_of_enis as (
  select
    recorded_at,
    account_id,
    NETWORK_INTERFACE_ID as eni_id,
    PRIVATE_IP_ADDRESS as private_ip,
    CASE
      when interface_type = 'interface'
      and ATTACHMENT is NULL
      and GROUPS like '%eks%' THEN 'kubernetes'
      when description like 'ELB %' THEN 'network_load_balancer'
      when interface_type = 'interface'
      and ATTACHMENT ['PublicDnsName'] LIKE 'ec2-%'
      AND GROUPS is not NULL then 'ec2'
      when PRIVATE_IP_ADDRESSES like '%ec2.internal%' then 'ec2-instance'
      when PRIVATE_IP_ADDRESSES like '%compute.internal%' then 'ec2-instance'
      when interface_type = 'interface'
      and DESCRIPTION like 'AWS Lambda VPC ENI%'
      then 'lambda'
      else interface_type
    END as object_type,
    CASE
      when description != '' then description
      when interface_type = 'interface'
        and ATTACHMENT ['PublicDnsName'] LIKE 'ec2-%'
        AND GROUPS is not NULL then 'ec2-instance'
      when instance_id is not NULL then 'ec2-instance'
      else 'unknown'
    END as object_name,
    COALESCE(instance_id, 'config') as eni_instance_id,
    'config' as source
  from
    AWS_CONFIG
  left outer join (select value, instance_id from aws_instances, lateral flatten(input => details['NetworkInterfaces'])) on value['NetworkInterfaceId'] = NETWORK_INTERFACE_ID
  where
    RESOURCE_TYPE = 'AWS::EC2::NetworkInterface'
  UNION
  SELECT
    recorded_at,
    account_id,
    eni_id,
    private_ip,
    object_type,
    object_name,
    eni_instance_id,
    source
  from
    elb_network_interface_cloudtrail
  UNION
  SELECT
    recorded_at,
    account_id,
    eni_id,
    private_ip,
    'ec2-instance' as object_type,
    'ec2-instance' as object_name,
    eni_instance_id::varchar as eni_instance_id,
    'run_instance_cloudtrail' as source
  from
    run_instances_enis
),
eni_grouping as (
  select
    min(recorded_at) as window_start,
    max(recorded_at) as last_recorded,
    account_id,
    eni_id,
    min(private_ip) as private_ip,
    min(object_type) as object_type,
    max(object_name) as object_name,
    max(eni_instance_id) as eni_instance_id,
    max(source) as source
  FROM
    union_of_enis
  group by
    ENI_ID,
    account_id
)
select
  *,
  lead(window_start) over (
    partition by private_ip
    order by
      window_start
  ) as window_end
from
  eni_grouping

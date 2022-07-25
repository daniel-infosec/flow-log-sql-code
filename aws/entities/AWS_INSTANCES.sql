-- Start off with gathering EC2 instances FROM AWS config
-- Assuming that recorded_at is when we ingested the data into our DB
-- Assuming that V is the full JSON object
-- Much of this code was from Maria Polyakova
WITH aws_config_raw AS (
  SELECT
    account_id,
    value as details,
    resource_id AS instance_id,
    value:privateIpAddress::string AS ip_address,
    recorded_at
  FROM
    aws_config, lateral flatten(configuration:networkInterfaces)
  WHERE
    resource_type = 'AWS::EC2::Instance'
    AND recorded_at >= dateadd(day, -30, current_timestamp)
),
-- Get latest and earliest time we've seen instances FROM config data
ec2_config_instances AS (
  SELECT
    'AWS_CONFIG' AS flag,
    account_id,
    instance_id,
    MAX(recorded_at) AS latest_recorded,
    MIN(recorded_at) AS earliest_recorded
  FROM
    aws_config_raw
  GROUP BY
    flag,
    account_id,
    instance_id
),
-- Get EC2 instance details FROM cloudtrail RunInstances
cloudtrail_raw AS (
  SELECT
    DISTINCT recipient_account_id AS account_id,
    value AS details,
    details:instanceId::String AS INSTANCE_ID,
    details:"privateIpAddress"::String AS IP_ADDRESS,
    recorded_at
  FROM
    cloudtrail,
    LATERAL FLATTEN (input => response_elements:instancesSet:items)
  WHERE
    event_name = 'RunInstances'
    AND recorded_at >= dateadd(day, -30, current_timestamp)
),
-- Get max and min recorded at
cloudtrail_instance AS (
  SELECT
    'CLOUDTRAIL' AS flag,
    account_id,
    instance_id,
    max(recorded_at) AS latest_recorded,
    min(recorded_at) AS earliest_recorded
  FROM
    CLOUDTRAIL_RAW
  group by
    flag,
    account_id,
    instance_id
),
-- Union cloudtrail data with config data
all_instances AS (
  SELECT
    flag,
    account_id,
    instance_id,
    latest_recorded,
    earliest_recorded
  FROM
    cloudtrail_instance
  UNION
  ALL
  SELECT
    flag,
    account_id,
    instance_id,
    latest_recorded,
    earliest_recorded
  FROM
    ec2_config_instances
),
-- Get the earliest recorded time
get_earliest_recorded AS (
  SELECT
    flag,
    A.account_id,
    A.instance_id,
    latest_recorded,
    earliest_recorded,
    true_earliest_recorded
  FROM
    all_instances A
    JOIN (
      SELECT
        instance_id,
        account_id,
        MAX(latest_recorded) AS true_latest_recorded,
        MIN(earliest_recorded) AS true_earliest_recorded
      FROM
        all_instances
      GROUP BY
        instance_id,
        account_id
    ) B ON A.instance_id = B.instance_id
    AND A.account_id = B.account_id
    AND A.latest_recorded = B.true_latest_recorded
),
helper AS (
  SELECT
    A.account_id,
    A.details,
    A.INSTANCE_ID,
    A.IP_ADDRESS,
    A.recorded_at,
    flag,
    -- See the included flatten_array.js file
    -- https://docs.snowflake.com/en/developer-guide/udf/javascript/udf-javascript-introduction.html
    flatten_array(DETAILS:Tags, 'Key', 'Value') AS tags,
    RECORDED_AT AS LATEST_AVAILABLE_DATA,
    IFF(
      true_earliest_recorded < DETAILS:"LaunchTime",
      true_earliest_recorded,
      DETAILS:"LaunchTime"
    ) AS LAUNCH_TIME
  FROM
    aws_config_raw A
    JOIN (
      SELECT
        *
      FROM
        get_earliest_recorded
      WHERE
        FLAG = 'AWS_CONFIG'
    ) B on A.INSTANCE_ID = B.INSTANCE_ID
    AND A.ACCOUNT_ID = B.ACCOUNT_ID
    AND A.RECORDED_AT = B.latest_recorded
  UNION
  ALL
  SELECT
    A.account_id,
    A.details,
    A.INSTANCE_ID,
    A.IP_ADDRESS,
    A.recorded_at,
    flag,
    -- See the included flatten_array.js file
    -- https://docs.snowflake.com/en/developer-guide/udf/javascript/udf-javascript-introduction.html
    flatten_array(DETAILS:tagSet:items, 'key', 'value') AS tags,
    RECORDED_AT AS LATEST_AVAILABLE_DATA,
    IFF(
      true_earliest_recorded < DETAILS:"launchTime",
      true_earliest_recorded,
      DETAILS:"launchTime"
    ) AS LAUNCH_TIME
  FROM
    CLOUDTRAIL_RAW A
    JOIN (
      SELECT
        *
      FROM
        get_earliest_recorded
      WHERE
        FLAG = 'CLOUDTRAIL'
    ) B on A.INSTANCE_ID = B.INSTANCE_ID
    AND A.ACCOUNT_ID = B.ACCOUNT_ID
    AND A.RECORDED_AT = B.latest_recorded
),
helper_two AS (
  select
    max(account_id) AS account_id,
    max(details) AS details,
    instance_id,
    ip_address,
    max(recorded_at) AS recorded_at,
    max(flag) AS flag,
    max(tags) AS tags,
    max(latest_available_data) AS latest_available_data,
    max(launch_time) AS launch_time
  FROM
    helper
  group by
    3,
    4
),
-- Get termination events FROM Cloudtrail
term AS (
  SELECT
    DISTINCT recipient_account_id AS account_id,
    value:instanceId::String AS instance_id,
    max(event_time) AS recorded_at
  FROM
    cloudtrail,
    LATERAL FLATTEN (input => RESPONSE_ELEMENTS:instancesSet:items)
  WHERE
    event_time >= DATEADD(day, -30, CURRENT_TIMESTAMP)
    and EVENT_NAME IN ('StopInstances', 'TerminateInstances')
  group by
    1,
    2
)
select
  a.*,
  COALESCE(
    B.RECORDED_AT,
    lead(launch_time) over (
      partition by ip_address
      order by
        launch_time ASc
    )
  ) AS window_end
FROM
  helper_two a
  left outer JOIN TERM b on a.instance_id = b.instance_id
  and a.recorded_at::date <= b.recorded_at::date

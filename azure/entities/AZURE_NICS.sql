-- Get NIC creation events
with nic_create_events_raw AS (
    SELECT
        parse_json(properties['responseBody']) AS response_body,
        response_body['name']::STRING AS name,
        response_body['location']::STRING AS nic_location,
        response_body['properties'] AS nic_properties,
        response_body['tags'] AS tags,
        response_body['type']::STRING AS type,
        *
    FROM
        azure_logs
    WHERE
        operation_name = 'MICROSOFT.NETWORK/NETWORKINTERFACES/WRITE'
        AND recorded_at >= dateadd(day, -30, current_timestamp())
        AND result_signature = 'Succeeded.Created'
),
-- Group to remove any potential duplicates
nic_create_events AS (
    SELECT
        raw['tenantId']::string AS tenant_id,
        split(properties['entity'], '/')[2]::string AS subscription_id,
        properties['entity']::STRING AS nic_id,
        name,
        nic_location,
        nic_properties,
        tags,
        type,
        min(event_time) AS earliest,
        max(event_time) AS latest
    FROM
        nic_create_events_raw
    GROUP BY
        tenant_id,
        subscription_id,
        id,
        name,
        nic_location,
        nic_properties,
        tags,
        type
),
-- Get NICs FROM collection info
nic_collect_raw AS (
    SELECT
        tenant_id::STRING AS tenant_id,
        subscription_id::STRING AS subscription_id,
        id::STRING AS nic_id,
        name::STRING AS name,
        location::STRING AS nic_location,
        properties AS nic_properties,
        tags,
        type::STRING AS type,
        min(recorded_at) AS earliest,
        max(recorded_at) AS latest
    FROM
        -- sourced from https://github.com/snowflakedb/SnowAlert/blob/master/src/connectors/azure_collect.py
        -- I'm sure you can modify this to use something like https://github.com/cloudquery/cloudquery
        azure_collect_network_interfaces
    WHERE
        recorded_at >= dateadd(day, -30, current_timestamp())
    GROUP BY
        tenant_id,
        subscription_id,
        id,
        name,
        nic_location,
        nic_properties,
        tags,
        type
),
-- Union audit log NICs with collection NICs
all_nics AS (
    SELECT
        lower(tenant_id) AS tenant_id,
        lower(subscription_id) AS subscription_id,
        lower(id) AS id,
        lower(name) AS name,
        tags,
        lower(nic_location) AS nic_location,
        nic_properties AS nic_properties,
        lower(type) AS type,
        earliest,
        latest,
        'collect_script' AS source
    FROM
        nic_collect_raw
    UNION
    SELECT
        LOWER(tenant_id) AS tenant_id,
        LOWER(subscription_id) AS subscription_id,
        LOWER(id) AS id,
        LOWER(name) AS name,
        tags,
        LOWER(nic_location) AS nic_location,
        nic_properties AS nic_properties,
        LOWER(type) AS type,
        earliest,
        latest,
        'operation_logs' AS source
    FROM
        nic_create_events
),
-- Get the earliest seen for the NICs
final_nics AS (
    SELECT
        a.*,
        true_earliest
    FROM
        all_nics a
        join (
            SELECT
                id,
                min(earliest) AS true_earliest
            FROM
                all_nics
            GROUP BY
                id
        ) b on a.id = b.id
),
-- Get IP information via lateral flatten
flattened_nics AS (
    SELECT
        tenant_id::VARCHAR AS tenant_id,
        subscription_id::VARCHAR AS subscription_id,
        id::VARCHAR AS id,
        name::VARCHAR AS name,
        nic_location::VARCHAR AS nic_location,
        parse_json(nic_properties)::VARIANT AS nic_properties,
        tags::VARIANT AS tags,
        type::VARCHAR AS type,
        source::VARCHAR AS source,
        earliest,
        value AS ip_configuration,
        true_earliest AS window_start
    FROM
        final_nics,
        lateral flatten(parse_json(nic_properties)['ipConfigurations'])
),
-- Remove potential duplicates
remove_duplicates AS (
    with helper AS (
        SELECT
            ROW_NUMBER() OVER(
                PARTITION BY ID
                ORDER BY
                    TAGS desc
            ) AS rn,
            *
        FROM
            flattened_nics
    )
    SELECT
        *
    FROM
        helper
    WHERE
        RN = 1
),
-- Get NIC termination events
term AS (
    SELECT
        properties ['entity'] AS nic_id,
        event_time AS term_time,
        *
    FROM
        azure_logs
    WHERE
        operation_name = 'MICROSOFT.NETWORK/NETWORKINTERFACES/DELETE'
        AND recorded_at >= dateadd(day, -30, current_timestamp())
        AND result_signature = 'Succeeded.'
        and result_type = 'Success'
),
-- Get when the NIC was last seen via partition
window_end_temp AS (
    SELECT
        tenant_id,
        subscription_id,
        id AS nic_id,
        name AS nic_name,
        nic_location,
        nic_properties,
        tags AS nic_tags,
        type,
        source,
        earliest,
        ip_configuration,
        window_start,
        lead(window_start) over (
            partition by ip_configuration ['properties'] ['privateIPAddress']
            ORDER BY
                earliest asc
        ) AS window_end_temp
    FROM
        remove_duplicates
)
SELECT
    tenant_id,
    subscription_id,
    nic_id,
    nic_name,
    nic_location,
    nic_properties,
    nic_tags,
    type,
    source,
    earliest,
    ip_configuration,
    window_start,
    coalesce(term_time, window_end_temp) AS window_end
FROM
    window_end_temp
    LEFT OUTER JOIN term on lower(window_end_temp.nic_id) = lower(term.nic_id)

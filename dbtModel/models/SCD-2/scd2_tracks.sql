{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='source_id',
    indexes=[{'columns': ['track_id']}]
) }}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS track_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}

-- Reset sequence on full refresh
{% if not is_incremental() %}
    {% set reset_sequence_query %}
        ALTER SEQUENCE track_id_seq RESTART WITH 1;
    {% endset %}
    {% do run_query(reset_sequence_query) %}
{% endif %}

WITH src_data AS (
    SELECT
        id,
        name,
        artist_names,
        artist_ids,
        album_id,
        album_name
    FROM {{ source('spotify', 'tracks') }}
),

deduplicated_src AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY id DESC) AS row_num 
        FROM src_data
    ) subquery
    WHERE row_num = 1
),

intermediate AS (
    SELECT
        nextval('track_id_seq') AS track_id,
        id AS source_id,
        name,
        artist_names,
        artist_ids,
        album_id,
        album_name,
        CURRENT_TIMESTAMP::timestamp with time zone AS valid_from,
        NULL::timestamp with time zone AS valid_to,
        TRUE AS is_current
    FROM deduplicated_src
)

{% if is_incremental() %}
, existing_data AS (
    SELECT
        track_id,
        source_id,
        name,
        artist_names,
        artist_ids,
        album_id,
        album_name,
        valid_from,
        valid_to,
        is_current
    FROM {{ this }}
),
new_records AS (
    SELECT
        i.track_id,
        i.source_id,
        i.name,
        i.artist_names,
        i.artist_ids,
        i.album_id,
        i.album_name,
        i.valid_from,
        NULL::timestamp with time zone AS valid_to,
        TRUE AS is_current
    FROM intermediate i
    LEFT JOIN existing_data e ON i.source_id = e.source_id AND e.is_current = TRUE
    WHERE e.source_id IS NULL
       OR i.name <> e.name
       OR i.artist_names <> e.artist_names
       OR i.artist_ids <> e.artist_ids
       OR i.album_id <> e.album_id
       OR i.album_name <> e.album_name
),
closed_out_old_records AS (
    SELECT
        e.track_id,
        e.source_id,
        e.name,
        e.artist_names,
        e.artist_ids,
        e.album_id,
        e.album_name,
        e.valid_from,
        CURRENT_TIMESTAMP::timestamp with time zone AS valid_to,
        FALSE AS is_current
    FROM existing_data e
    JOIN intermediate i ON e.source_id = i.source_id
    WHERE e.is_current = TRUE
      AND (
            i.name <> e.name
         OR i.artist_names <> e.artist_names
         OR i.artist_ids <> e.artist_ids
         OR i.album_id <> e.album_id
         OR i.album_name <> e.album_name
      )
)

SELECT * FROM new_records
UNION ALL
SELECT * FROM closed_out_old_records

{% else %}

SELECT * FROM intermediate

{% endif %}
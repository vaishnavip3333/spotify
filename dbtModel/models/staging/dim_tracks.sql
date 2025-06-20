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
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY id DESC) AS row_num -- Removed effective_date
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
        album_name
    FROM deduplicated_src
),

final AS (
    SELECT
        intermediate.track_id, 
        intermediate.source_id, 
        intermediate.name, 
        intermediate.artist_names,
        intermediate.artist_ids, 
        intermediate.album_id, 
        intermediate.album_name 
    FROM intermediate

    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS existing
        ON intermediate.source_id = existing.source_id
    WHERE
        existing.source_id IS NULL
        OR intermediate.name <> existing.name
        OR intermediate.artist_names <> existing.artist_names
        OR intermediate.artist_ids <> existing.artist_ids
        OR intermediate.album_id <> existing.album_id
        OR intermediate.album_name <> existing.album_name
    {% endif %}
)

SELECT * FROM final
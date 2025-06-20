{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='source_id',
    indexes=[{'columns': ['artist_id']}],
    pre_hook=["ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_artist_id"],
    post_hook=["ALTER TABLE {{ this }} ADD CONSTRAINT pk_artist_id PRIMARY KEY (artist_id)"]
) }}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS artist_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}

-- Reset sequence on full refresh
{% if not is_incremental() %}
    {% set reset_sequence_query %}
        ALTER SEQUENCE artist_id_seq RESTART WITH 1;
    {% endset %}
    {% do run_query(reset_sequence_query) %}
{% endif %}

WITH src_data AS (
    SELECT
        id,
        name,
        genres,
        popularity,
        source
    FROM {{ source('spotify', 'artists') }}
),

intermediate AS (
    SELECT
        nextval('artist_id_seq') AS artist_id,
        id AS source_id,
        name,
        genres,
        popularity,
        source
    FROM src_data
),

final AS (
    SELECT
        intermediate.artist_id, 
        intermediate.source_id, 
        intermediate.name,
        intermediate.genres, 
        intermediate.popularity, 
        intermediate.source 
    FROM intermediate

    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS existing
        ON intermediate.source_id = existing.source_id
    WHERE
        existing.source_id IS NULL
        OR intermediate.name <> existing.name
        OR intermediate.genres <> existing.genres
        OR intermediate.popularity <> existing.popularity
        OR intermediate.source <> existing.source
    {% endif %}
)

SELECT * FROM final
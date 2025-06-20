{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_id',
        indexes=[{'columns': ['album_id']}],
        pre_hook=[
            "ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS pk_album_id"
        ],
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT pk_album_id PRIMARY KEY (album_id)"
        ]
    )
}}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS album_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}

{% if not is_incremental() %}
    {% set reset_sequence_query %}
        ALTER SEQUENCE album_id_seq RESTART WITH 1;
    {% endset %}
    {% do run_query(reset_sequence_query) %}
{% endif %}

WITH src_data AS (
    SELECT *
    FROM {{ source('spotify', 'albums') }}
),

artists AS (
    SELECT *
    FROM {{ ref('dim_artists') }}
),

intermediate AS (
    SELECT
        nextval('album_id_seq') AS album_id,
        src.id AS source_id,
        src.name,
        src.artist_names,
        src.artist_ids,
        src.popularity,
        artists.name AS artist_name,
        artists.genres AS artist_genre
    FROM src_data AS src
    LEFT JOIN artists
        ON src.artist_ids = artists.source_id
),

final AS (
    SELECT
        intermediate.album_id,
        intermediate.source_id,
        intermediate.name,
        intermediate.artist_names,
        intermediate.artist_ids,
        intermediate.popularity,
        intermediate.artist_name,
        intermediate.artist_genre
    FROM intermediate

    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS existing
        ON intermediate.source_id = existing.source_id
    WHERE
        existing.source_id IS NULL
        OR intermediate.name <> existing.name
        OR intermediate.artist_names <> existing.artist_names
        OR intermediate.artist_ids <> existing.artist_ids
        OR intermediate.popularity <> existing.popularity
    {% endif %}
)

SELECT * FROM final

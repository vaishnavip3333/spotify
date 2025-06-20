{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='source_id',
    indexes=[{'columns': ['artist_id']}],
    pre_hook=[
        "ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS pk_artist_idd"
    ],
    post_hook=[
        "ALTER TABLE {{ this }} ADD CONSTRAINT pk_artist_idd PRIMARY KEY (artist_id)"
    ]
) }}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS artist_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}

{% if not is_incremental() %}
    {% set reset_sequence_query %}
        ALTER SEQUENCE artist_id_seq RESTART WITH 1;
    {% endset %}
    {% do run_query(reset_sequence_query) %}
{% endif %}

with artist_data as (
    select
        nextval('artist_id_seq') as artist_id,
        id as source_id,
        name,
        genres,
        popularity,
        source,
        current_timestamp::timestamp with time zone as valid_from
    from {{ source('spotify', 'artists') }}
),

{% if is_incremental() %}
existing_data as (
    select
        artist_id,
        source_id,
        name,
        genres,
        popularity,
        source,
        valid_from,
        is_current
    from {{ this }}
),
{% endif %}

new_records as (
    select
        a.artist_id,
        a.source_id,
        a.name,
        a.genres,
        a.popularity,
        a.source,
        a.valid_from,
        null as valid_to,
        true as is_current
    from artist_data a

    {% if is_incremental() %}
    left join existing_data e on a.source_id = e.source_id
    where e.source_id is null
       or a.name <> e.name
       or a.genres <> e.genres
       or a.popularity <> e.popularity
       or a.source <> e.source
    {% endif %}
)

{% if is_incremental() %}
, closed_out_old_records as (
    select
        e.artist_id,
        e.source_id,
        e.name,
        e.genres,
        e.popularity,
        e.source,
        e.valid_from,
        current_timestamp::timestamp with time zone as valid_to,
        false as is_current
    from existing_data e
    join artist_data a on e.source_id = a.source_id
    where e.is_current = true
      and (
            a.name <> e.name
         or a.genres <> e.genres
         or a.popularity <> e.popularity
         or a.source <> e.source
      )
)
{% endif %}

select * from new_records

{% if is_incremental() %}
union all
select * from closed_out_old_records
{% endif %}

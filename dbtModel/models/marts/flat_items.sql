WITH
albums AS (
    SELECT * FROM {{ ref('dim_albums') }} 
),
artists AS (
    SELECT * FROM {{ ref('dim_artists') }} 
),
tracks AS (
    SELECT * FROM {{ ref('dim_tracks') }} 
)

SELECT
    t.source_id, 
    t.name,
    t.artist_names,
    t.artist_ids,
    t.album_name AS track_album_name, 
    t.album_id,

    a.name AS album_name, 

    ar.name AS artist_name,
    ar.genres,
    ar.popularity,
    ar.source

FROM tracks t
LEFT JOIN albums a ON CAST(t.album_id AS bigint) = a.album_id 
LEFT JOIN artists ar ON t.artist_ids = ar.source_id 
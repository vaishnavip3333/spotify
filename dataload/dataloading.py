import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()


DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "schema": os.getenv("SCHEMA_NAME")
}


engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)


with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DB_CONFIG['schema']}"))


def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def upload_to_db(df, table_name):
    df.to_sql(
        name=table_name,
        con=engine,
        schema=DB_CONFIG["schema"],
        if_exists="replace",
        index=False
    )
    print(f"Uploaded: {table_name} ({len(df)} rows)")


def process_albums(path):
    albums = load_json(path)
    records = []
    for album in albums:
        records.append({
            "id": album["id"],
            "name": album["name"],
            "release_date": album["release_date"],
            "total_tracks": album["total_tracks"],
            "popularity": album["popularity"],
            "artist_names": ', '.join([a["name"] for a in album.get("artists", [])]),
            "artist_ids": ', '.join([a["id"] for a in album.get("artists", [])]),
            "track_ids": ', '.join([t["id"] for t in album.get("tracks", [])]) if "tracks" in album else None,
            "track_names": ', '.join([t["name"] for t in album.get("tracks", [])]) if "tracks" in album else None,
            **extract_metadata(album)
        })
    df = pd.DataFrame(records)
    upload_to_db(df, "albums")


def process_artists(path):
    artists = load_json(path)
    records = []
    for artist in artists:
        records.append({
            "id": artist["id"],
            "name": artist["name"],
            "genres": ', '.join(artist.get("genres", [])),
            "popularity": artist["popularity"],
            **extract_metadata(artist)
        })
    df = pd.DataFrame(records)
    upload_to_db(df, "artists")


def process_tracks(path):
    tracks = load_json(path)
    records = []
    for track in tracks:
        album = track.get("album", {})
        records.append({
            "id": track["id"],
            "name": track["name"],
            "artist_names": ', '.join([a["name"] for a in track.get("artist", [])]),
            "artist_ids": ', '.join([a["id"] for a in track.get("artist", [])]),
            "album_id": album.get("id"),
            "album_name": album.get("name"),
            "duration_ms": track["duration_ms"],
            "explicit": track["explicit"],
            "popularity": track["popularity"],
            **extract_metadata(track)
        })
    df = pd.DataFrame(records)
    upload_to_db(df, "tracks")


def extract_metadata(record):
    return {
        "extraction_datetime": record.get("extraction_datetime"),
        "source": record.get("source"),
        "extractor": record.get("extractor", "unknown"),
        "data_version": record.get("data_version"),
        "timezone": record.get("timezone")
    }


if __name__ == "__main__":
    BASE_PATH = "/home/vaishnavi/spotify/extract"
    process_albums(f"{BASE_PATH}/albums.json")
    process_artists(f"{BASE_PATH}/artists.json")
    process_tracks(f"{BASE_PATH}/tracks.json")

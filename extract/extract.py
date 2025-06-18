import os
import json
import asyncio
import aiohttp
import random
import logging
import datetime
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import Spotify
from aiohttp import ClientSession, ClientTimeout, TCPConnector


load_dotenv()
CLIENT_ID = os.getenv("client_id")
CLIENT_SECRET = os.getenv("client_secret")


sp_sync = Spotify(auth_manager=SpotifyClientCredentials(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
))


ARTISTS, ALBUMS, TRACKS = [], [], []
semaphore = asyncio.Semaphore(4)


def get_metadata():
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    return {
        "extraction_datetime": now.isoformat(),
        "source": "Spotify API v1",
        "extractor": "spotipy",
        "data_version": "1.0",
        "timezone": str(now.tzinfo)
    }


async def fetch(session, url, headers, max_retries=3):
    for attempt in range(1, max_retries + 1):
        async with semaphore:
            await asyncio.sleep(2 ** (attempt - 1) + random.uniform(0.1, 0.5))
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", "1"))
                        logging.warning(f"Rate limited (429). Retrying in {retry_after}s | Attempt {attempt}")
                        await asyncio.sleep(retry_after)
                        continue
                    elif response.status != 200:
                        logging.error(f"Error {response.status} on {url} | Attempt {attempt}")
                        return None
                    return await response.json()
            except aiohttp.ClientError as e:
                logging.error(f"Client error: {e} | Attempt {attempt}")
            except Exception as e:
                logging.exception(f"Unexpected error: {e} | Attempt {attempt}")
    logging.critical(f"Failed to fetch {url} after {max_retries} attempts")
    return None

async def fetch_artist_details(session, artist_id, headers):
    return await fetch(session, f"https://api.spotify.com/v1/artists/{artist_id}", headers)


async def fetch_artist_albums(session, artist_id, headers):
    return await fetch(session, f"https://api.spotify.com/v1/artists/{artist_id}/albums?limit=5&include_groups=album", headers)


async def fetch_album(session, album_id, headers):
    return await fetch(session, f"https://api.spotify.com/v1/albums/{album_id}", headers)


async def fetch_track(session, track_id, headers):
    return await fetch(session, f"https://api.spotify.com/v1/tracks/{track_id}", headers)


async def process_artist(session, artist_id, headers, metadata):
    try:
        artist = await fetch_artist_details(session, artist_id, headers)
        if not artist or artist.get("popularity", 0) <= 80:
            return

        ARTISTS.append({
            "id": artist["id"],
            "name": artist["name"],
            "genres": artist["genres"],
            "popularity": artist["popularity"],
            **metadata
        })

        await asyncio.sleep(random.uniform(0.2, 0.5))
        albums_data = await fetch_artist_albums(session, artist_id, headers)
        if not albums_data:
            return

        album_ids = set()
        for album in albums_data.get("items", []):
            if album["id"] in album_ids:
                continue
            album_ids.add(album["id"])

            full_album = await fetch_album(session, album["id"], headers)
            if not full_album:
                continue

            album_tracks = []
            for track in full_album.get("tracks", {}).get("items", []):
                full_track = await fetch_track(session, track["id"], headers)
                if not full_track:
                    continue

                TRACKS.append({
                    "id": full_track["id"],
                    "name": full_track["name"],
                    "artist": [{"id": a["id"], "name": a["name"]} for a in full_track["artists"]],
                    "album": {"id": full_album["id"], "name": full_album["name"]},
                    "duration_ms": full_track["duration_ms"],
                    "explicit": full_track["explicit"],
                    "popularity": full_track["popularity"],
                    **metadata
                })

                album_tracks.append({
                    "id": full_track["id"],
                    "name": full_track["name"]
                })

            ALBUMS.append({
                "id": full_album["id"],
                "name": full_album["name"],
                "release_date": full_album["release_date"],
                "total_tracks": full_album["total_tracks"],
                "popularity": full_album["popularity"],
                "artists": [{"id": a["id"], "name": a["name"]} for a in full_album["artists"]],
                "tracks": album_tracks,
                **metadata
            })

            await asyncio.sleep(random.uniform(0.3, 0.6))
    except Exception as e:
        logging.error(f"Error processing artist {artist_id}: {e}")


async def main():
    try:
        token = SpotifyClientCredentials(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        ).get_access_token(as_dict=False)

        headers = {"Authorization": f"Bearer {token}"}
        metadata = get_metadata()

        async with ClientSession(
            connector=TCPConnector(limit=10),
            timeout=ClientTimeout(total=60)
        ) as session:
            search_terms = ["a", "e", "i", "m", "pop", "rock"]
            artist_ids = set()

            for term in search_terms:
                results = sp_sync.search(q=term, type="artist", limit=50)
                for artist in results["artists"]["items"]:
                    if artist["popularity"] > 80:
                        artist_ids.add(artist["id"])

            tasks = [process_artist(session, artist_id, headers, metadata) for artist_id in artist_ids]
            await asyncio.gather(*tasks)

       
        with open("artists.json", "w") as f:
            json.dump(ARTISTS, f, indent=2)

        with open("albums.json", "w") as f:
            json.dump(ALBUMS, f, indent=2)

        with open("tracks.json", "w") as f:
            json.dump(TRACKS, f, indent=2)

    except Exception as e:
        logging.error(f"Unexpected error in main: {e}")


if __name__ == "__main__":
    asyncio.run(main())

import os
import time
import pandas as pd
from dotenv import load_dotenv
import requests

# --------------------------- CONFIG ---------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")
LANG = "en-US"
BASE_URL = "https://api.themoviedb.org/3"
IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

# Config for downloading data
PAGES_PER_ENDPOINT = 30        # Number of pages per endpoint (20 films per page)
SLEEP_BETWEEN_REQUESTS = 0.2  # timeout between requests
SAVE_FILE = "tmdb_movies_full.csv"

# Endpoint
ENDPOINTS = [
    "/movie/popular",
    "/movie/top_rated",
    "/movie/upcoming",
    "/movie/now_playing"
]

# Create session
def create_session():
    """Create session to reuse connection"""
    session = requests.Session()

    # A. Set default Headers for the entire session
    # All requests from this session will automatically have this Token
    session.headers.update({
        "accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    })

    # B. Setting default parameters (Option)
    # Helps you avoid typing "?language=en" at the end of each URL
    session.params = {
        "language": "en"
    }
    return session

def safe_get(session, url, params=None):
    """Safe API calls with retry"""
    for _ in range(3):
        try:
            r = session.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
        except:
            pass
        time.sleep(1)
    return None

def get_genres(session):
    """Get category list"""
    url = f"{BASE_URL}/genre/movie/list"
    data = safe_get(session, url)
    genre_map = {}
    if data and "genres" in data:
        for g in data["genres"]:
            genre_map[g["id"]] = g["name"]
    return genre_map

def get_movies_from_endpoint(session, endpoint, pages=10):
    """Get a list of movies from an endpoint"""
    all_movies = []
    for page in range(1, pages + 1):
        params = {"page": page}
        data = safe_get(session, BASE_URL + endpoint, params)
        if not data or "results" not in data:
            break
        all_movies.extend(data["results"])
        print(f"📄 {endpoint} page {page} ({len(data['results'])} film)")
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return all_movies

def get_movie_credits(session, movie_id):
    """Get the top 5 actors and directors"""
    url = f"{BASE_URL}/movie/{movie_id}/credits"
    data = safe_get(session, url)
    if not data:
        return "", ""
    cast_names = [c["name"] for c in data.get("cast", [])[:5]]
    directors = [c["name"] for c in data.get("crew", []) if c.get("job") == "Director"]
    top_cast = ", ".join(cast_names)
    director = directors[0] if directors else ""
    return top_cast, director

def main():
    if not API_KEY or API_KEY == "YOUR_API_KEY_HERE":
        print("❌ Please set API_KEY or edit directly in code.")
        return

    session = create_session()

    print("🎬 Getting category list...")
    genre_map = get_genres(session)
    print(f"✅ Fetched {len(genre_map)} genres.\n")

    all_movies = []
    seen = set()

    for endpoint in ENDPOINTS:
        print(f"📥 Get data from {endpoint} ...")
        movies = get_movies_from_endpoint(session, endpoint, pages=PAGES_PER_ENDPOINT)
        for m in movies:
            if m["id"] not in seen:
                all_movies.append(m)
                seen.add(m["id"])

    print(f"\nTotal number of films obtained: {len(all_movies)}")

    rows = []
    for i, m in enumerate(all_movies, 1):
        mid = m.get("id")
        title = m.get("title")
        overview = m.get("overview")
        release_date = m.get("release_date")
        vote_average = m.get("vote_average")
        genres = [genre_map.get(g, g) for g in m.get("genre_ids", [])]
        poster = IMAGE_BASE + m["poster_path"] if m.get("poster_path") else ""

        top_cast, director = get_movie_credits(session, mid)

        rows.append({
            "id": mid,
            "title": title,
            "overview": overview,
            "release_date": release_date,
            "vote_average": vote_average,
            "genres": ", ".join(genres),
            "top_cast": top_cast,
            "director": director,
            "poster_url": poster
        })

        print(f"🎞️ {i}/{len(all_movies)}: {title}")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    df = pd.DataFrame(rows)
    df.to_csv(SAVE_FILE, index=False, encoding="utf-8-sig")
    print(f"\n✅ Data saved to {SAVE_FILE} ({len(df)} film).")


if __name__ == "__main__":
    main()



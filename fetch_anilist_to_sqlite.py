#!/usr/bin/env python3
import argparse #* Used to handle command line arguments 
import json 
import re #* Regular Expressions ( pattern matching and string cleaning )
import sqlite3 
import time 
from typing import Dict, Any, List #* Improves readability, shows clean and modern python code

import requests #* For making HTTP requests 
from tqdm import tqdm #* Provides a progress bar for loops

ANILIST_URL = "https://graphql.anilist.co"

# To access API from Anilist, GraphQL query is used...
# GraphQL lets you ask for exactly the fields you need.
GRAPHQL_QUERY = """
query ($page: Int, $perPage: Int){
    Page(page: $page, perPage: $perPage){
        pageInfo{
            currentPage
            hasNextPage
        }
        media(type: ANIME, sort: POPULARITY_DESC) {
            id
            title{
                romaji
                english
            }
            description
            genres
            tags{
                name
            }
            averageScore
            studios(isMain: true){
                nodes{
                    name
                }
            }
        }
    }
}
"""

def clean_description(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)

    text = (text.replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", '"')
                .replace("&#39;", "'"))
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def fetch_page(page: int, per_page: int = 50) -> Dict[str, Any]:
    variables = {"page":page, "perPage": per_page}
    resp = requests.post(ANILIST_URL, json={"query": GRAPHQL_QUERY, "variables": variables}, timeout = 30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Anilist returned errors: {data['errors']}")
    
    return data["data"]["Page"]

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anime (
                 id INTEGER PRIMARY KEY,
                 title_romaji TEXT,
                 title_english TEXT,
                 description TEXT,
                 genres TEXT,   -- JSON array string
                 tags TEXT,     --JSON array string
                 average_score INTEGER,
                 studios TEXT   --JSON array string (main studios)
                 )
    """)
    conn.commit()

def upsert_batch(conn: sqlite3.Connection, rows: List[Dict[str, Any]])-> None:
    conn.executemany("""
        INSERT INTO anime (id, title_romaji, title_english, description, genres, tags, average_score, studios)
        VALUES (:id, :title_romaji, :title_english, :description, :genres, :tags, :average_score, :studios)
        ON CONFLICT(id) DO UPDATE SET
            title_romaji = excluded.title_romaji,
            title_english = excluded.title_english,
            description = excluded.description,
            genres = excluded.genres,
            tags = excluded.tags,
            average_score = excluded.average_score,
            studios = excluded.studios
    """, rows)
    conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Fetch Anilist popular anime -> SQLite")
    parser.add_argument("--pages", type=int, default=10, help="Number of pages to fetch (each page has up to 50 items)")
    parser.add_argument("--per-page", type=int, default=50, help="Items per page (Anilist max is 50)")
    parser.add_argument("--db", type=str, default="anilist.db", help="SQLite database filename")
    parser.add_argument("--sleep", type=float, default=0.5, help ="Sleep seconds between requests (stay friendly to API)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    ensure_schema(conn)

    total_inserted = 0
    has_next = True
    page = 1

    with tqdm(total=args.pages, desc="Fetching Pages", unit="page") as pbar:
        while has_next and page <= args.pages:
            try:
                payload = fetch_page(page=page, per_page=args.per_page)
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                wait = 5 if status == 429 else 3
                tqdm.write(f"HTTP error (status {status}). Retrying Page {page} after {wait}s...")
                time.sleep(wait)
                continue

            media = payload.get("media", [])
            rows = []
            for m in media:
                studios_nodes = (m.get("studios") or {}).get("nodes") or []
                studio_names = [s.get("name") for s in studios_nodes if s and s.get("name")]
                tags_nodes = m.get("tags") or []
                tag_names = [t.get("name") for t in tags_nodes if t and t.get("name")]

                row = {
                    "id": m.get("id"),
                    "title_romaji": (m.get("title") or {}).get("romaji"),
                    "title_english": (m.get("title") or {}).get("english"),
                    "description": clean_description(m.get("description") or ""),
                    "genres": json.dumps(m.get("genres") or [], ensure_ascii=False),
                    "tags": json.dumps(tag_names, ensure_ascii=False),
                    "average_score": m.get("averageScore"),
                    "studios": json.dumps(studio_names, ensure_ascii=False),
                }

                if row["id"] is not None:
                    rows.append(row)
            
            if rows:
                upsert_batch(conn, rows)
                total_inserted += len(rows)

            has_next = (payload.get("pageInfo") or {}).get("hasNextPage", False)
            page += 1
            pbar.update(1)
            time.sleep(args.sleep)
    tqdm.write(f"Done. Inserted/updated ~{total_inserted} rows into {args.db}.")

if __name__ == "__main__":
    main()
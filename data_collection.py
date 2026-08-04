"""
YouTube Data Analysis - Phase 1: Data Collection
Niche: Travel (India-focused)

Pipeline:
1. Search videos across keywords (from config.py)
2. Extract unique channel_ids from those videos
3. Fetch channel details, filter to country == 'IN'
4. Keep only videos belonging to India-based channels
5. Fetch full video details for the filtered videos
6. Save channels + videos to SQLite DB (and CSV backups)
"""

import sqlite3
import pandas as pd
import isodate
import time
import os
import logging
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import config

os.makedirs(config.DATA_DIR, exist_ok=True)

# ---- Logging setup ----
LOG_PATH = f"{config.DATA_DIR}/phase1.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

youtube = build("youtube", "v3", developerKey=config.API_KEY)


def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Step A: Search videos for a single keyword (handles pagination)
# ---------------------------------------------------------------------------
def search_videos_for_keyword(keyword, max_results):
    videos = []
    next_page_token = None

    while len(videos) < max_results:
        request = youtube.search().list(
            part="snippet",
            q=keyword,
            type="video",
            order="viewCount",
            maxResults=min(50, max_results - len(videos)),
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response.get("items", []):
            videos.append({
                "video_id": item["id"]["videoId"],
                "channel_id": item["snippet"]["channelId"],
                "search_keyword": keyword
            })

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return videos


# ---------------------------------------------------------------------------
# Step B: Get full video details (stats + duration) for a batch of video IDs
# ---------------------------------------------------------------------------
def get_video_details(video_id_keyword_map):
    video_ids = list(video_id_keyword_map.keys())
    all_details = []

    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(chunk)
        )
        response = request.execute()

        for item in response.get("items", []):  # fixed: was "itemas"
            vid = item["id"]

            try:
                duration_min = isodate.parse_duration(
                    item["contentDetails"]["duration"]
                ).total_seconds() / 60
            except Exception:
                duration_min = None

            all_details.append({
                "video_id": vid,
                "channel_id": item["snippet"]["channelId"],
                "title": item["snippet"]["title"],
                "description": item["snippet"].get("description", "")[:500],
                "published_at": item["snippet"]["publishedAt"],
                "duration_min": round(duration_min, 2) if duration_min else None,
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0)),
                "comments_count": int(item["statistics"].get("commentCount", 0)),
                "tags": ",".join(item["snippet"].get("tags", [])),
                "search_keyword": video_id_keyword_map.get(vid, ""),
                "fetched_at": get_timestamp()
            })

        time.sleep(0.2)

    return all_details


# ---------------------------------------------------------------------------
# Step C: Get channel details for a batch of channel IDs
# ---------------------------------------------------------------------------
def get_channel_details(channel_ids):
    channel_ids = list(set(channel_ids))
    all_channels = []

    for i in range(0, len(channel_ids), 50):
        chunk = channel_ids[i:i + 50]
        request = youtube.channels().list(
            part="snippet,statistics",
            id=",".join(chunk)
        )
        response = request.execute()

        for item in response.get("items", []):
            all_channels.append({
                "channel_id": item["id"],
                "channel_title": item["snippet"]["title"],
                "subscribers": int(item["statistics"].get("subscriberCount", 0)),
                "total_videos": int(item["statistics"].get("videoCount", 0)),
                "total_views": int(item["statistics"].get("viewCount", 0)),
                "country": item["snippet"].get("country", "Unknown"),
                "created_date": item["snippet"]["publishedAt"],
                "fetched_at": get_timestamp()
            })

        time.sleep(0.2)

    return all_channels


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main():
    # ---- Step 1: Search videos across all keywords ----
    all_search_results = []
    for kw in config.keywords:
        logger.info(f"Searching videos for keyword: {kw}")
        results = search_videos_for_keyword(kw, max_results=config.MAX_RESULTS_PER_KEYWORD)
        all_search_results.extend(results)
        logger.info(f"Collected {len(results)} videos for '{kw}' | total so far: {len(all_search_results)}")

    search_df = pd.DataFrame(all_search_results).drop_duplicates(subset="video_id")
    logger.info(f"Total unique videos found: {len(search_df)}")

    # ---- Step 2: Get channel details, filter to India ----
    unique_channel_ids = search_df["channel_id"].unique().tolist()
    logger.info(f"Unique channels to check: {len(unique_channel_ids)}")

    channel_details = get_channel_details(unique_channel_ids)
    channels_df = pd.DataFrame(channel_details)

    channels_df = channels_df[channels_df["country"] == "IN"].reset_index(drop=True)
    logger.info(f"India-based channels after filtering: {len(channels_df)}")

    # ---- Step 3: Filter videos to only India-based channels ----
    search_df = search_df[search_df["channel_id"].isin(channels_df["channel_id"])].reset_index(drop=True)
    logger.info(f"Videos remaining after country filter: {len(search_df)}")

    video_id_keyword_map = dict(zip(search_df["video_id"], search_df["search_keyword"]))

    # ---- Step 4: Get full video details for filtered videos ----
    video_details = get_video_details(video_id_keyword_map)
    videos_df = pd.DataFrame(video_details)
    logger.info(f"Final videos_df row count: {len(videos_df)}")

    # ---- Step 5: Save to database + CSV backups ----
    conn = sqlite3.connect(config.DB_PATH)
    channels_df.to_sql("channels", conn, if_exists="replace", index=False)
    videos_df.to_sql("videos", conn, if_exists="replace", index=False)
    conn.close()

    channels_df.to_csv(config.CHANNELS_CSV, index=False)
    videos_df.to_csv(config.VIDEOS_CSV, index=False)

    logger.info("Phase 1 complete. Data saved to database and CSV.")
    return channels_df, videos_df


if __name__ == "__main__":
    channels_df, videos_df = main()
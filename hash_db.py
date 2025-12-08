# File: hashtag.py
# Merged: your TikTokApi pipeline + Selenium per-video stats fallback
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from TikTokApi import TikTokApi
from sqlalchemy import MetaData, Table, create_engine, update
from sqlalchemy.orm import sessionmaker

import app  # uses app.db, app.app, your models

# ------------------------
# Config & Logging
# ------------------------
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Prefer env vars; fallback to literal if present
MS_TOKEN = os.getenv("TIKTOK_MS_TOKEN", "dG__F6-Fn6wR6vLV-1Pq8S_MOA92hXCOaGO1-EhRRTytukPpxWp-ojXUO8qruu0fTbykFRnkflMRTex0XFboPYc-8lz7jx7BjMD9ZvTv3Fc5rcG8Sm3paUE8mE_TaGgDm9dTU7xG-6T_Tg==")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
metadata = MetaData()  # moved to module scope

# ------------------------
# Selenium (fallback) setup
# ------------------------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, JavascriptException

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAVE_WDM = True
except Exception:
    HAVE_WDM = False


def _build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1440,900")
    opts.add_argument("--lang=en-US")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    # trim obvious automation flags (why: some sites gate content)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    if HAVE_WDM:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
    else:
        driver = webdriver.Chrome(options=opts)

    driver.set_page_load_timeout(45)
    return driver


def _parse_abbrev_count(s: str) -> int:
    if not s:
        return -1
    s = s.strip().upper().replace(",", "").replace(" ", "")
    m = re.match(r"^([0-9]*\.?[0-9]+)([KMB])?$", s)
    if m:
        num = float(m.group(1))
        suf = m.group(2)
        mult = 1
        if suf == "K":
            mult = 1_000
        elif suf == "M":
            mult = 1_000_000
        elif suf == "B":
            mult = 1_000_000_000
        return int(num * mult)
    if s.isdigit():
        return int(s)
    return -1


def _extract_from_sigi_state(text: Optional[str], video_id: Optional[str]) -> Tuple[int, int, int, int, int]:
    if not text or not video_id:
        return -1, -1, -1, -1, -1
    try:
        js = json.loads(text)
        item = js.get("ItemModule", {}).get(video_id, {})
        stats = item.get("stats", {})
        play = int(stats.get("playCount", -1))
        like = int(stats.get("diggCount", -1))
        comment = int(stats.get("commentCount", -1))
        share = int(stats.get("shareCount", -1))
        favorite = int(stats.get("collectCount", -1))
        return play, like, comment, share, favorite
    except Exception:
        return -1, -1, -1, -1, -1


def _visible_counters(driver: webdriver.Chrome) -> Tuple[int, int, int, int, int]:
    selectors: Dict[str, List[Tuple[By, str]]] = {
        "like": [
            (By.CSS_SELECTOR, "[data-e2e='like-count']"),
            (By.XPATH, "//*[contains(., 'Like') or contains(., 'love')]/span"),
        ],
        "comment": [
            (By.CSS_SELECTOR, "[data-e2e='comment-count']"),
            (By.XPATH, "//*[contains(., 'Comment')]/span"),
        ],
        "share": [
            (By.CSS_SELECTOR, "[data-e2e='share-count']"),
            (By.XPATH, "//*[contains(., 'Share')]/span"),
        ],
        "play": [
            (By.CSS_SELECTOR, "[data-e2e='video-views']"),
            (By.XPATH, "//*[contains(., 'views') or contains(., 'Views')]/span"),
        ],
        "favorite": [
            (By.CSS_SELECTOR, "[data-e2e='favorite-count']"),
            (By.CSS_SELECTOR, "[data-e2e='collect-count']"),
            (By.XPATH, "//*[contains(., 'Save') or contains(., 'Favorite') or contains(., 'Bookmark') or contains(., 'Collect')]/span"),
        ],
    }

    def first_text(pairs: List[Tuple[By, str]]) -> Optional[str]:
        for by, sel in pairs:
            try:
                el = WebDriverWait(driver, 3).until(EC.presence_of_element_located((by, sel)))
                txt = (el.text or "").strip()
                if txt:
                    return txt
            except Exception:
                continue
        return None

    like = _parse_abbrev_count(first_text(selectors["like"]) or "")
    comment = _parse_abbrev_count(first_text(selectors["comment"]) or "")
    share = _parse_abbrev_count(first_text(selectors["share"]) or "")
    play = _parse_abbrev_count(first_text(selectors["play"]) or "")
    favorite = _parse_abbrev_count(first_text(selectors["favorite"]) or "")
    return play, like, comment, share, favorite


def _get_video_id(url: str) -> Optional[str]:
    m = re.search(r"/video/(\d+)", url)
    return m.group(1) if m else None


def get_stats_via_web(url: str, headless: bool = True, retries: int = 2, wait: int = 12) -> Tuple[int, int, int, int, int]:
    """Returns (play, like, comment, share, favorite) from the video detail page."""
    driver = _build_driver(headless=headless)
    base = None
    try:
        driver.execute_script("window.open(arguments[0], '_blank', 'noopener')", "about:blank")
        WebDriverWait(driver, 5).until(EC.number_of_windows_to_be(1))
        base = driver.current_window_handle

        for attempt in range(retries + 1):
            try:
                driver.get(url)
                # wait for SIGI_STATE or page ready
                try:
                    WebDriverWait(driver, wait).until(
                        lambda d: d.execute_script(
                            "return !!document.getElementById('SIGI_STATE') || document.readyState === 'complete';"
                        )
                    )
                except TimeoutException:
                    pass

                text = None
                try:
                    text = driver.execute_script(
                        "var s=document.getElementById('SIGI_STATE'); return s ? s.textContent : null;"
                    )
                except JavascriptException:
                    pass

                vid = _get_video_id(url)
                play, like, comment, share, favorite = _extract_from_sigi_state(text, vid)

                if min(play, like, comment, share, favorite) < 0:
                    p2, l2, c2, s2, f2 = _visible_counters(driver)
                    play = p2 if play < 0 else play
                    like = l2 if like < 0 else like
                    comment = c2 if comment < 0 else comment
                    share = s2 if share < 0 else share
                    favorite = f2 if favorite < 0 else favorite

                return play, like, comment, share, favorite
            except Exception as e:
                logger.warning(f"Selenium attempt {attempt+1} failed: {e}")
                if attempt >= retries:
                    return -1, -1, -1, -1, -1
                time.sleep(1.0)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ------------------------
# TikTokApi pipeline
# ------------------------
class HashInfo:
    hashkey = []

    @staticmethod
    async def get_hashtag_videos(hashtag_names: Iterable[str], use_web_fallback: bool = True,
                                 headless: bool = True, max_web_hits_per_min: int = 20) -> None:
        throttle_interval = 60.0 / max(1, max_web_hits_per_min)
        last_web_hit = 0.0

        async with TikTokApi() as api:
            # create one session up front
            await api.create_sessions(ms_tokens=[MS_TOKEN], num_sessions=1, sleep_after=3, headless=False)

            for source in [''.join(name) for name in hashtag_names]:
                HashInfo.hashkey = source
                logger.debug(f"Processing hashtag: {HashInfo.hashkey}")

                tag = api.hashtag(name=HashInfo.hashkey)
                hashtag_data: List[dict] = []

                async for video in tag.videos(count=150):
                    hashtag_data.append(video.as_dict)

                # Persist
                with app.app.app_context():
                    app.db.create_all()

                    # link hashtag id from your table
                    results = app.db.session.query(app.TikTokHashKey.id).filter(
                        app.TikTokHashKey.hash_name == HashInfo.hashkey
                    ).all()
                    ids = [row.id for row in results]
                    if ids:
                        hash_id = int(ids[0])
                    else:
                        logger.warning(f"No TikTokHashKey.id found for {HashInfo.hashkey}")
                        continue

                    for item in hashtag_data:
                        # --- Extract fields from API
                        vid = item["id"]
                        created = datetime.utcfromtimestamp(item["createTime"])
                        duration = item["video"]["duration"]
                        desc = item.get("desc") or ""
                        author_id = item["author"]["id"]
                        author_nickname = item["author"]["nickname"]
                        author_uniqueId = item["author"]["uniqueId"]

                        author_stats = item.get("authorStats", {}) or {}
                        a_digg = author_stats.get("diggCount", 0)
                        a_follower = author_stats.get("followerCount", 0)
                        a_following = author_stats.get("followingCount", 0)
                        a_friend = author_stats.get("friendCount", 0)
                        a_heart_total = author_stats.get("heart", 0)
                        a_video_count = author_stats.get("videoCount", 0)

                        stats = item.get("stats", {}) or {}
                        play = stats.get("playCount", -1)
                        like = stats.get("diggCount", -1)     # like/love
                        comment = stats.get("commentCount", -1)
                        share = stats.get("shareCount", -1)
                        favorite = stats.get("collectCount", -1)  # save/favorite

                        # --- Build URLs (fix: use uniqueId in video URL, no trailing tuple comma)
                        user_url = f"https://www.tiktok.com/@{author_uniqueId}"
                        video_url = f"https://www.tiktok.com/@{author_uniqueId}/video/{vid}"

                        # --- Fallback via Selenium if any count missing
                        needs_fallback = any(
                            v is None or (isinstance(v, int) and v < 0) for v in [play, like, comment, share, favorite]
                        )
                        if use_web_fallback and needs_fallback:
                            dt = time.time() - last_web_hit
                            if dt < throttle_interval:
                                await asyncio.sleep(throttle_interval - dt)
                            p2, l2, c2, s2, f2 = get_stats_via_web(video_url, headless=headless)
                            last_web_hit = time.time()
                            play = p2 if (play is None or play < 0) else play
                            like = l2 if (like is None or like < 0) else like
                            comment = c2 if (comment is None or comment < 0) else comment
                            share = s2 if (share is None or share < 0) else share
                            favorite = f2 if (favorite is None or favorite < 0) else favorite

                        # --- Upsert video row
                        hashtag_video = app.TikTokVideosInfo(
                            s_id=hash_id,
                            video_id=vid,
                            source_id=author_id,
                            video_createtime=created,
                            video_description=desc,
                            video_url=video_url,
                            video_author=author_nickname,
                            video_duration=duration,
                            video_collectcount=favorite,
                            video_commentcount=comment,
                            video_diggcount=like,
                            video_playcount=play,
                            video_sharecount=share,
                        )

                        existing = app.db.session.query(app.TikTokVideosInfo).filter(
                            app.TikTokVideosInfo.video_id == vid
                        ).first()

                        if existing is None:
                            try:
                                app.db.session.add(hashtag_video)
                                await asyncio.sleep(0.3)
                                app.db.session.commit()
                                logger.info(f"Added {HashInfo.hashkey} :: video {vid}")

                                # map to all_content (uses module-scope metadata now)
                                content_table = Table('all_content', metadata, autoload_with=engine)
                                columns = content_table.columns.keys()
                                content_column = columns[1]
                                network_column = columns[2]
                                content_id = app.db.session.query(app.TikTokVideosInfo.id).filter(
                                    app.TikTokVideosInfo.video_id == vid
                                ).scalar()
                                if content_id:
                                    insert_stmt = content_table.insert().values([{content_column: content_id, network_column: 5}])
                                    app.db.session.execute(insert_stmt)
                                    app.db.session.commit()
                                    logger.info(f"Linked content_id={content_id} → network_id=5")
                            except Exception as e:
                                logger.error(f"Insert error: {e}")
                                app.db.session.rollback()
                        else:
                            try:
                                upd = update(app.TikTokVideosInfo).where(
                                    app.TikTokVideosInfo.video_id == vid
                                ).values(
                                    s_id=hash_id,
                                    source_id=author_id,
                                    video_createtime=created,
                                    video_description=desc,
                                    video_url=video_url,
                                    video_author=author_nickname,
                                    video_duration=duration,
                                    video_collectcount=favorite,
                                    video_commentcount=comment,
                                    video_diggcount=like,
                                    video_playcount=play,
                                    video_sharecount=share,
                                )
                                app.db.session.execute(upd)
                                await asyncio.sleep(0.1)
                                app.db.session.commit()
                                logger.info(f"Updated {HashInfo.hashkey} :: video {vid}")
                            except Exception as e:
                                logger.error(f"Update error: {e}")
                                app.db.session.rollback()

                        # --- Upsert author row
                        user_row = app.TikTokUsersInfo(
                            source_id=author_id,
                            user_nickname=author_nickname,
                            user_uniqueId=author_uniqueId,
                            user_diggcount=a_digg,
                            user_followercount=a_follower,
                            user_followingcount=a_following,
                            user_friendcount=a_friend,
                            user_heart=a_heart_total,
                            user_videocount=a_video_count,
                            user_url=user_url,
                        )

                        exists_user = app.db.session.query(app.TikTokUsersInfo).filter(
                            app.TikTokUsersInfo.source_id == author_id
                        ).first()
                        if exists_user:
                            try:
                                upd = update(app.TikTokUsersInfo).where(
                                    app.TikTokUsersInfo.source_id == author_id
                                ).values(
                                    user_nickname=author_nickname,
                                    user_uniqueId=author_uniqueId,
                                    user_diggcount=a_digg,
                                    user_followercount=a_follower,
                                    user_followingcount=a_following,
                                    user_friendcount=a_friend,
                                    user_heart=a_heart_total,
                                    user_videocount=a_video_count,
                                    user_url=user_url,
                                )
                                app.db.session.execute(upd)
                                await asyncio.sleep(0.05)
                                app.db.session.commit()
                            except Exception as e:
                                logger.error(f"User update error: {e}")
                                app.db.session.rollback()
                        else:
                            try:
                                app.db.session.add(user_row)
                                await asyncio.sleep(0.05)
                                app.db.session.commit()
                            except Exception as e:
                                logger.error(f"User insert error: {e}")
                                app.db.session.rollback()


# ------------------------
# Entrypoint
# ------------------------
if __name__ == "__main__":
    # Query hashtags to process (your table + filter)
    session = Session()
    users_tbl = Table('tbl_tk_hashtag_sources', metadata, autoload_with=engine)

    # You used: app.TikTokHashKey.hash_check == True
    # all_hashtag = session.query(users_tbl).with_entities(app.TikTokHashKey.hash_name).filter(
    #     app.TikTokHashKey.hash_check == True
    # ).all()

    # # For demo: 5 random or a fixed example
    # if all_hashtag:
    #     rand_hash = random.sample(all_hashtag, min(5, len(all_hashtag)))
    #     sources = [''.join(row) for row in rand_hash]
    # else:
    #     sources = ["အာကာသပြတိုက်"]

    sources = ["အာကာသပြတိုက်ရန်ကုန်"]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(HashInfo.get_hashtag_videos(sources, use_web_fallback=True, headless=True, max_web_hits_per_min=20))
    loop.close()
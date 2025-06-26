# File: tiktok-video-content/ind.py

from TikTokApi import TikTokApi
import asyncio
import json
import os
import logging
import random

from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy import update
import app
from datetime import datetime

# Proxy configuration
# PROXY_HOST = "192.168.11.203"
# PROXY_PORT = 3128
# PROXY_USERNAME = "user208"
# PROXY_PASSWORD = "user208"

# Configure proxy URL
# proxy_url = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)

# ms_token = os.environ.get(
#     "c1kM6tsndCrK6RySzHXVln62T0pXC6Hi3tTqLWmdA1WwMJ7zET2jO-nca_Z9UzwaRxs8PhFesTPaU41iyuAGkvP37hGlxXwLGO6AIlRECHZMfg42eVVbkE-0C8uzvU0rdOVrh2ILefIKgWyVLqQEMVaQnw==", None
# )
ms_token = os.environ.get("ms_token", None)
class UserInfo:
    o_data = []
    vo_data = []
    source_name = []

    async def user_profile_data(all_users):
        async with TikTokApi() as api:
            await api.create_sessions(
                    ms_tokens=[ms_token],
                    num_sessions=1,
                    sleep_after=5,
                    headless=True,
                    # proxies=[proxy_url]
                )
            user = api.user("popularnewsjournal")
            user_data = await user.info()
            followerCount = user_data["userInfo"]["stats"].get("followerCount")
            followingCount = user_data["userInfo"]["stats"].get("followingCount")
            friendCount = user_data["userInfo"]["stats"].get("friendCount")
            heartCount = user_data["userInfo"]["stats"].get("heartCount")
            post_count = user_data["userInfo"]["stats"].get("videoCount")
            logging.info(f"User {UserInfo.source_name} has follower count {followerCount}, following count {followingCount}, friend count {friendCount}, heart count {heartCount}, post count {post_count}.")

            user_videos = []
            r_data = json.dumps(user_data, indent=4)
            UserInfo.o_data = json.loads(r_data)

            async for video in user.videos(count=30):
                user_videos.append(video.as_dict)
            v_data = json.dumps(user_videos, indent=4)
            UserInfo.vo_data = json.loads(v_data)

            await insert_video()

async def insert_video():
    UserInfo.source = UserInfo.source_name 
    with app.app.app_context():
        app.db.create_all()

        for select_data in UserInfo.vo_data:
            video_id = select_data["id"]
            source_id = UserInfo.o_data["userInfo"]["user"]["id"]
            video_createtime = select_data["createTime"]
            video_description = str(select_data["desc"])
            video_url = "https://www.tiktok.com/@{}/video/{}".format("popularnewsjournal", select_data["id"])
            
            video_author = "orginal" if "authorName" not in select_data["music"] else select_data["music"]["authorName"]
            video_duration = 0 if "duration" not in select_data["music"] else select_data["music"]["duration"]
            video_music_title = "original" if "title" not in select_data["music"] else select_data["music"]["title"]

            video_collectcount = select_data["stats"]["collectCount"]
            video_commentcount = select_data["stats"]["commentCount"]
            video_diggcount = select_data["stats"]["diggCount"]
            video_playcount = select_data["stats"]["playCount"]
            video_sharecount = select_data["stats"]["shareCount"]

            users_videos = app.TikTokVideosInfo(
                video_id=video_id,
                source_id=source_id,
                video_createtime=datetime.utcfromtimestamp(video_createtime),
                video_description=video_description,
                video_url=video_url,
                video_author=video_author,
                video_duration=video_duration,
                video_music_title=video_music_title,
                video_collectcount=video_collectcount,
                video_commentcount=video_commentcount,
                video_diggcount=video_diggcount,
                video_playcount=video_playcount,
                video_sharecount=video_sharecount,
            )

            result_video = app.db.session.query(app.TikTokVideosInfo).filter(app.TikTokVideosInfo.video_id == video_id).first()

            if result_video is None:
                try:
                    app.db.session.add(users_videos)
                    await asyncio.sleep(3)
                    app.db.session.commit()
                    logging.info(f"video data source is added successful, source id : {source_id}, video id : {video_id}, comment : {video_commentcount}, collect : {video_collectcount}, play : {video_playcount}, share : {video_sharecount}")

                    content = app.db.session.query(app.TikTokVideosInfo.id).filter(app.TikTokVideosInfo.video_id == video_id).all()
                    ids = [row.id for row in content]
                    content_table = Table("all_content", metadata, autoload_with=engine)
                    columns = content_table.columns.keys()
                    content_column = columns[1]
                    network_column = columns[2]

                    values_to_insert = [{content_column: content_id, network_column: 5} for content_id in ids]
                    insert_allcontent = content_table.insert().values(values_to_insert)
                    app.db.session.execute(insert_allcontent)
                    logging.debug(f"Added content id values : {ids} for network id 5")

                except Exception as e:
                    print(f"Error updating data: {e}")
                    app.db.session.rollback()

            else:
                update_user_videos = (
                    update(app.TikTokVideosInfo)
                    .where(app.TikTokVideosInfo.video_id == video_id)
                    .values(
                        source_id=source_id,
                        video_createtime=datetime.utcfromtimestamp(video_createtime),
                        video_description=video_description,
                        video_url=video_url,
                        video_author=video_author,
                        video_duration=video_duration,
                        video_music_title=video_music_title,
                        video_collectcount=video_collectcount,
                        video_commentcount=video_commentcount,
                        video_diggcount=video_diggcount,
                        video_playcount=video_playcount,
                        video_sharecount=video_sharecount,
                    )
                )

                try:
                    app.db.session.execute(update_user_videos)
                    await asyncio.sleep(3)
                    app.db.session.commit()
                    logging.info(f"video data source is updated successful, source id : {source_id}, video id : {video_id}, comment : {video_commentcount}, collect : {video_collectcount}, play : {video_playcount}, share : {video_sharecount}")
                except Exception as e:
                    print(f"Error updating data: {e}")
                    app.db.session.rollback()

if __name__ == "__main__":
    metadata = MetaData()
    users = Table("tbl_tk_sources", metadata, autoload_with=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    all_users = session.query(users).with_entities(app.TikTokSources.source_name).all()
    rand_source = random.sample(all_users, 7)
    sources = ["".join(user) for user in rand_source]
    logging.debug(f"Fetching sources from db : {sources}")

    loop = asyncio.get_event_loop()
    loop.run_until_complete((UserInfo.user_profile_data(sources)))
    loop.close()
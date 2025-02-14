from TikTokApi import TikTokApi
import asyncio
import json
import os
import logging
import random

from sqlalchemy import create_engine, MetaData, Table, update
from sqlalchemy.orm import sessionmaker
import app
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

ms_token = os.environ.get(
    "_csgYPYQLIRVbllarU0NFaEzFBLkg8vBxHRu-0NYyQD8Kcl66CDWa9LIc7uE83O1oAIT7nHExABopKP--09jhxxlWDBGWpxsbOmBGQSUa7FPsyWpsaZWaVD98WBlEYi4J74-AJmhx1i6LkTsyT0zbIs=", None
)
database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)

class UserInfo:
    o_data = []
    vo_data = []
    source = []
    source_name = ""

    async def user_profile_data(all_users):
        logging.debug(f"Starting user_profile_data with users: {all_users}")
        async with TikTokApi() as api:
            sources = ["".join(user) for user in all_users]
            for source in sources:
                UserInfo.source = source
                logging.debug(f"Processing source: {source}")
                await api.create_sessions(
                    ms_tokens=[ms_token],
                    num_sessions=1,
                    sleep_after=5,
                    headless=False,
                )
                
                user = api.user(UserInfo.source)
                user_data = await user.info()
                followerCount = user_data["userInfo"]["stats"].get("followerCount")
                followingCount = user_data["userInfo"]["stats"].get("followingCount")
                friendCount = user_data["userInfo"]["stats"].get("friendCount")
                heartCount = user_data["userInfo"]["stats"].get("heartCount")
                post_count = user_data["userInfo"]["stats"].get("videoCount")
                logging.debug(f"User {UserInfo.source} has follower count {followerCount}, following count {followingCount}, friend count {friendCount}, heart count {heartCount}, post count {post_count}.")

                user_videos = []
                r_data = json.dumps(user_data, indent=4)
                UserInfo.o_data = json.loads(r_data)

                async for video in user.videos(count=20):
                    user_videos.append(video.as_dict)
                v_data = json.dumps(user_videos, indent=4)
                UserInfo.vo_data = json.loads(v_data)

                await insert_video()

async def insert_video():
    with app.app.app_context():
        app.db.create_all()

        for select_data in UserInfo.vo_data:
            video_id = select_data["id"]
            source_id = UserInfo.o_data["userInfo"]["user"]["id"]
            video_createtime = select_data["createTime"]
            video_description = str(select_data["desc"])
            video_url = "https://www.tiktok.com/@{}/video/{}".format(UserInfo.source, select_data["id"])

            video_author = select_data["music"].get("authorName", "original")
            video_duration = select_data["music"].get("duration", 0)
            video_music_title = select_data["music"].get("title", "original")
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

            result_video = (
                app.db.session.query(app.TikTokVideosInfo)
                .filter(app.TikTokVideosInfo.video_id == video_id)
                .first()
            )

            if result_video:
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
                app.db.session.execute(update_user_videos)
                await asyncio.sleep(5)
                app.db.session.commit()
                logging.debug(
                    f"Updated video id: {video_id}, source id: {source_id},create time: {datetime.utcfromtimestamp(video_createtime)}"
                )
            else:
                app.db.session.add(users_videos)
                await asyncio.sleep(5)
                app.db.session.commit()
                logging.debug(
                    f"Added video id: {video_id}, source id: {source_id},create time: {datetime.utcfromtimestamp(video_createtime)}"
                )

async def check_and_crawl_users():
    with app.app.app_context():
        app.db.create_all()
        users = app.TikTokSources.query.filter(app.TikTokSources.owner == 1)&(app.TikTokSources.source_check == True).all()
        if len(users) > 10:
            users = random.sample(users, 10)
        sources = ["".join(user.source_name) for user in users]
        await UserInfo.user_profile_data(sources)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_and_crawl_users())
    loop.close()
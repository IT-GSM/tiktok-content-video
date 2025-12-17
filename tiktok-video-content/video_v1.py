from TikTokApi import TikTokApi
import asyncio
import json
import os
import logging
import random
import sys

from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker

from sqlalchemy import update
import app
from datetime import datetime, timedelta
from TikTokApi.exceptions import EmptyResponseException

# ms_token = os.environ.get(
#    "ms_token", None
# )  # set your own ms_token, think it might need to have visited a profile
# database_url = "postgresql://postgres:admin@localhost:5432/dbtiktok"
# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)

# get your own ms_token from your cookies on tiktok.com
# ms_token = "-iAlEuHbfFrb_4zcxo-XXHUEcAN7T0C8ru69KglJCZPDJMI2vQ3eeehTZ2AlkFwIayJoYFqKpCjDrbC4Qs0nj2TvzUg7ErgIHyzsRtbFZCcuV6uaRS7e0zVJLg9U8JJ80Xn6lrpO6IiuH8w-0fIRggs="
# ms_token2 = "PVppEY-qxjY94JUwVn3u9yzXa2cT7Clr3tnojbcmojFVwpF3Ok5YEU7btvPtDPuo66G-93-6pwU0zAJIm2_Or7n1VbbE4wqCl-OaQ1NRSVBzR_SzTrB6z3uNK6DmtqJ7Y5_KzPiVvpe81V2wuPc7VRQ="
# ms_token1 = "MmrwOPItMMBAC0fV0fcH7x4DUGfdMqEBUfhxSHE46dhe_Vj-0_hO-NR99pY6aQpu7AEXuHEFc54T8FgFSx7M6v77C-hlDiF19f1kAykF8YcJFwfutZL6WkkZY65ijWD-69AcfTlrCWdMjph1VEefSO4="

# ms_token = os.environ.get(
#     "c1kM6tsndCrK6RySzHXVln62T0pXC6Hi3tTqLWmdA1WwMJ7zET2jO-nca_Z9UzwaRxs8PhFesTPaU41iyuAGkvP37hGlxXwLGO6AIlRECHZMfg42eVVbkE-0C8uzvU0rdOVrh2ILefIKgWyVLqQEMVaQnw==", None
# )
# ms_token = os.environ.get("ms_token", None)
def get_nested(data, *keys):
        """Safely get nested dictionary keys, returns None if any key is missing."""
        for key in keys:
            if not isinstance(data, dict) or key not in data:
                return None
            data = data[key]
        return data

def update_crawl_status(source_name, status):
    """Update the crawl status for a specific source."""
    with app.app.app_context():
        try:
            update_stmt = (
                update(app.TikTokSources)
                .where(app.TikTokSources.source_name == source_name)
                .values(
                    crawl_status=status,
                    crawl_status_updated=datetime.utcnow()
                )
            )
            app.db.session.execute(update_stmt)
            app.db.session.commit()
            logging.info(f"Updated crawl status for {source_name} to {status}")
        except Exception as e:
            logging.error(f"Error updating crawl status for {source_name}: {e}")
            app.db.session.rollback()

def reset_expired_success_states():
    """Reset success states to onready if they are older than 4 hours."""
    with app.app.app_context():
        try:
            four_hours_ago = datetime.utcnow() - timedelta(hours=4)
            update_stmt = (
                update(app.TikTokSources)
                .where(
                    (app.TikTokSources.crawl_status == "success") &
                    (app.TikTokSources.crawl_status_updated < four_hours_ago)
                )
                .values(
                    crawl_status="onready",
                    crawl_status_updated=datetime.utcnow()
                )
            )
            result = app.db.session.execute(update_stmt)
            app.db.session.commit()
            if result.rowcount > 0:
                logging.info(f"Reset {result.rowcount} expired success states to onready")
        except Exception as e:
            logging.error(f"Error resetting expired success states: {e}")
            app.db.session.rollback()


class UserInfo:
    o_data = []
    vo_data = []
    source = []
    source_name = ""

    async def user_profile_data(all_users):
        logging.debug(f"Starting user_profile_data with users: {all_users}")
        
        # Set status to onprocess for all users being crawled
        for user_name in all_users:
            update_crawl_status(user_name, "onprocess")
        
        async with TikTokApi() as api:
            await api.create_sessions(
                ms_tokens=["eKceaaF8LSRg8q2vT8QePTPF8RD9j1Xl7tiekKtqC0kDU5yldprw_wNVHNrHYoDP0vjgsqb89-xYjKh7EhIE6vULnluXTmAfuwSNka8BbUltC7zQ8Cc6thWEFM4bvnYWdees4FY34mJ2Fw=="],
                num_sessions=1,
                sleep_after=5,
                headless=True,
            )

            for user_name in all_users:
                try:
                    # Set the current username for use in video_url and other fields
                    UserInfo.source = user_name
                    user = api.user(user_name)
                    try:
                        user_data = await user.info()
                        
                        # Check if we got an unexpected status code response or empty user data
                        if isinstance(user_data, dict) and 'userInfo' in user_data:
                            user_info = user_data.get('userInfo', {})
                            if isinstance(user_info, dict) and 'user' in user_info:
                                user_obj = user_info.get('user', {})
                                if not user_obj or not isinstance(user_obj, dict):
                                    logging.error(f"Got an unexpected status code or empty user data for '{user_name}': {user_data}")
                                    logging.warning(f"User '{user_name}' might be private, deleted, or have restricted access")
                                    update_crawl_status(user_name, "onready")
                                    continue
                        
                    except EmptyResponseException as e:
                        logging.error(f"TikTok returned an empty response for user info '{user_name}': {e}")
                        update_crawl_status(user_name, "onready")  # Reset to onready on error
                        continue

                    # Defensive: check for expected keys
                    if (
                        not user_data
                        or get_nested(user_data, "userInfo", "user") is None
                    ):
                        logging.error(f"User info missing 'user' key for '{user_name}': {user_data}")
                        update_crawl_status(user_name, "onready")  # Reset to onready on error
                        continue

                    UserInfo.o_data = user_data

                    followerCount = user_data["userInfo"]["stats"].get("followerCount")
                    followingCount = user_data["userInfo"]["stats"].get("followingCount")
                    friendCount = user_data["userInfo"]["stats"].get("friendCount")
                    heartCount = user_data["userInfo"]["stats"].get("heartCount")
                    post_count = user_data["userInfo"]["stats"].get("videoCount")
                    logging.info(
                        f"User {user_name} has follower count {followerCount}, following count {followingCount}, friend count {friendCount}, heart count {heartCount}, post count {post_count}."
                    )

                    user_videos = []

                    r_data = json.dumps(user_data, indent=4)
                    UserInfo.o_data = json.loads(r_data)

                    try:
                        async for video in user.videos(count=30):
                            user_videos.append(video.as_dict)
                    except EmptyResponseException as e:
                        logging.error(
                            f"TikTok returned an empty response for videos of '{user_name}': {e}"
                        )
                        update_crawl_status(user_name, "onready")  # Reset to onready on error
                        continue

                    if not user_videos:
                        logging.warning(
                            f"No videos found for user '{user_name}'. Skipping database insert."
                        )
                        update_crawl_status(user_name, "onready")  # Reset to onready on error
                        continue

                    v_data = json.dumps(user_videos, indent=4)
                    UserInfo.vo_data = json.loads(v_data)

                    await UserInfo.insert_video(user_name)
                    # await UserInfo.insert_user()
                    
                    # Set status to success after crawling is complete
                    update_crawl_status(user_name, "success")
                    
                except Exception as e:
                    logging.error(f"Unexpected error while crawling user '{user_name}': {e}")
                    update_crawl_status(user_name, "onready")  # Reset to onready on error
                    continue
    

    async def insert_video(user_name):
        with app.app.app_context():
            app.db.create_all()

            for select_data in UserInfo.vo_data:
                try:
                    video_id = get_nested(select_data, "id")
                    source_id = get_nested(UserInfo.o_data, "userInfo", "user", "id")
                    if video_id is None or source_id is None:
                        logging.error(f"Missing video_id or source_id, skipping video insert. Data: {select_data}")
                        continue
                except KeyError as e:
                    logging.error(f"KeyError accessing video data for user '{user_name}': {e}")
                    logging.error(f"Video data structure: {select_data}")
                    continue
                except Exception as e:
                    logging.error(f"Unexpected error processing video data for user '{user_name}': {e}")
                    logging.error(f"Video data structure: {select_data}")
                    continue
                video_createtime = get_nested(select_data, "createTime")
                video_description = str(get_nested(select_data, "desc") or "")
                video_url = f"https://www.tiktok.com/@{user_name}/video/{video_id}"

                music = get_nested(select_data, "music") or {}
                video_author = music.get("authorName", "orginal")
                video_duration = music.get("duration", 0)
                video_music_title = music.get("title", "original")

                stats = get_nested(select_data, "stats") or {}
                video_collectcount = stats.get("collectCount", 0)
                video_commentcount = stats.get("commentCount", 0)
                video_diggcount = stats.get("diggCount", 0)
                video_playcount = stats.get("playCount", 0)
                video_sharecount = stats.get("shareCount", 0)
                crawl_status = "onready"
                crawl_status_updated = datetime.utcnow()

                users_videos = app.TikTokVideosInfo(
                    video_id=video_id,
                    source_id=source_id,
                    video_createtime=datetime.utcfromtimestamp(video_createtime) if video_createtime else None,
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
                    crawl_status=crawl_status,
                    crawl_status_updated=crawl_status_updated,
                )

                # result_video = app.db.session.query(app.TikTokVideosInfo).filter(app.TikTokVideosInfo.video_id == video_id).first()
                # print(result_video)

                result_video = (
                    app.db.session.query(app.TikTokVideosInfo)
                    .filter(app.TikTokVideosInfo.video_id == video_id)
                    .first()
                )
                # print(result_video)

                if result_video is None:
                    try:
                        app.db.session.add(users_videos)
                        await asyncio.sleep(3)
                        app.db.session.commit()
                        logging.debug(
                            "video data source is added successful, video id : {},comment : {}, collect : {}, play : {}, share : {}".format(
                                video_id,
                                video_commentcount,
                                video_collectcount,
                                video_playcount,
                                video_sharecount,
                            )
                        )

                        # content_id collect from table tiktokvideo info
                        content = (
                            app.db.session.query(app.TikTokVideosInfo.id)
                            .filter(app.TikTokVideosInfo.video_id == video_id)
                            .all()
                        )

                        # Extracting the id values from the result
                        ids = [row.id for row in content]

                        # Reflect the  table from the database
                        content_table = Table(
                            "all_content", metadata, autoload_with=engine
                        )

                        # Access the columns of the "content" table
                        columns = content_table.columns.keys()

                        # Print the column names
                        content_column = columns[1]
                        network_column = columns[2]
                        # print("content_column {},network_column {}".format(ids,5))

                        # Define the values to insert
                        values_to_insert = [
                            {content_column: content_id, network_column: 5}
                            for content_id in ids
                        ]

                        # Create an insert statement
                        insert_allcontent = content_table.insert().values(
                            values_to_insert
                        )

                        # Execute the insert statement
                        app.db.session.execute(insert_allcontent)
                        logging.debug(
                            "Added content id values : {} for network id 5".format(ids)
                        )

                    except Exception as e:
                        print(f"Error updating data: {e}")
                        app.db.session.rollback()

                else:
                    update_user_videos = (
                        update(app.TikTokVideosInfo)
                        .where(app.TikTokVideosInfo.video_id == video_id)
                        .values(
                            source_id=source_id,
                            video_createtime=datetime.utcfromtimestamp(
                                video_createtime
                            ),
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
                        logging.debug(
                            "video data source is updated successful, video id : {},comment : {}, collect : {}, play : {}, share : {}".format(
                                video_id,
                                video_commentcount,
                                video_collectcount,
                                video_playcount,
                                video_sharecount,
                            )
                        )
                    except Exception as e:
                        print(f"Error updating data: {e}")
                        app.db.session.rollback()

    async def insert_user():
        with app.app.app_context():
            app.db.create_all()

            try:
                source_id = get_nested(UserInfo.o_data, "userInfo", "user", "id")
                if source_id is None:
                    logging.error("Missing 'user' key in userInfo, skipping user insert.")
                    logging.error(f"User data structure: {UserInfo.o_data}")
                    return
            except KeyError as e:
                logging.error(f"KeyError accessing user data: {e}")
                logging.error(f"User data structure: {UserInfo.o_data}")
                return
            except Exception as e:
                logging.error(f"Unexpected error accessing user data: {e}")
                logging.error(f"User data structure: {UserInfo.o_data}")
                return

            user_title = get_nested(UserInfo.o_data, "shareMeta", "title") or ""
            user_nickname = get_nested(UserInfo.o_data, "userInfo", "user", "nickname") or ""
            user_uniqueId = get_nested(UserInfo.o_data, "userInfo", "user", "uniqueId") or ""
            user_relation = get_nested(UserInfo.o_data, "userInfo", "user", "relation") or ""
            user_diggcount = get_nested(UserInfo.o_data, "userInfo", "stats", "diggCount") or 0
            user_followercount = get_nested(UserInfo.o_data, "userInfo", "stats", "followerCount") or 0
            user_followingcount = get_nested(UserInfo.o_data, "userInfo", "stats", "followingCount") or 0
            user_friendcount = get_nested(UserInfo.o_data, "userInfo", "stats", "friendCount") or 0
            user_heart = get_nested(UserInfo.o_data, "userInfo", "stats", "heart") or 0
            user_videocount = get_nested(UserInfo.o_data, "userInfo", "stats", "videoCount") or 0
            user_url = f"https://www.tiktok.com/@{user_uniqueId}"

            users = app.TikTokUsersInfo(
                source_id=source_id,
                user_title=user_title,
                user_nickname=user_nickname,
                user_uniqueId=user_uniqueId,
                user_relation=user_relation,
                user_diggcount=user_diggcount,
                user_followercount=user_followercount,
                user_followingcount=user_followingcount,
                user_friendcount=user_friendcount,
                user_heart=user_heart,
                user_videocount=user_videocount,
                user_url=user_url,
            )

            result = (
                app.db.session.query(app.TikTokUsersInfo)
                .filter(app.TikTokUsersInfo.user_uniqueId == UserInfo.source)
                .first()
            )

            if result is None:
                try:
                    app.db.session.add(users)
                    await asyncio.sleep(3)
                    app.db.session.commit()
                    print("user data source {} added successful.".format(user_uniqueId))
                except Exception as e:
                    print(f"Error updating data: {e}")
                    app.db.session.rollback()
            else:
                try:
                    user_data_update = (
                        update(app.TikTokUsersInfo)
                        .where(app.TikTokUsersInfo.user_uniqueId == user_uniqueId)
                        .values(
                            source_id=source_id,
                            user_title=user_title,
                            user_nickname=user_nickname,
                            user_uniqueId=user_uniqueId,
                            user_relation=user_relation,
                            user_diggcount=user_diggcount,
                            user_followercount=user_followercount,
                            user_followingcount=user_followingcount,
                            user_friendcount=user_friendcount,
                            user_heart=user_heart,
                            user_videocount=user_videocount,
                        )
                    )

                    app.db.session.execute(user_data_update)
                    app.db.session.commit()
                    print(
                        "user data source {} updated successful.".format(user_uniqueId)
                    )
                except Exception as e:
                    print(f"Error updating data: {e}")
                    app.db.session.rollback()

            app.db.session.close()

async def check_and_crawl_videos():
    # Reset expired success states to onready
    reset_expired_success_states()
    
    # metadata = MetaData()
    users = Table("tbl_tk_sources", metadata, autoload_with=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Only get users with onready status for crawling owner=1 RAP, owner=2 media, owner=3 USDP, owner=4 otherparty
        all_users = session.query(users).with_entities(app.TikTokSources.source_name).filter(
             
            (app.TikTokSources.source_check == True) & (app.TikTokSources.owner == 4) &
            (app.TikTokSources.crawl_status == "onready")
        ).all()
        all_users_list = [user.source_name for user in all_users]
        
        if not all_users_list:
            logging.info("No users with 'onready' status found for crawling")
            return
            
        sample_size = min(30, len(all_users_list))
        rand_source = random.sample(all_users_list, sample_size)
        print(rand_source)
        await UserInfo.user_profile_data(rand_source)
    finally:
        session.close()

if __name__ == "__main__":
    metadata = MetaData()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_and_crawl_videos())
    loop.close()
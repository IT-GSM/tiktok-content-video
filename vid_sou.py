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

# ms_token = os.environ.get(
#    "ms_token", None
# )  # set your own ms_token, think it might need to have visited a profile
# database_url = "postgresql://postgres:admin@localhost:5432/dbtiktok"
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)

# get your own ms_token from your cookies on tiktok.com
# ms_token = "-iAlEuHbfFrb_4zcxo-XXHUEcAN7T0C8ru69KglJCZPDJMI2vQ3eeehTZ2AlkFwIayJoYFqKpCjDrbC4Qs0nj2TvzUg7ErgIHyzsRtbFZCcuV6uaRS7e0zVJLg9U8JJ80Xn6lrpO6IiuH8w-0fIRggs="
# ms_token2 = "PVppEY-qxjY94JUwVn3u9yzXa2cT7Clr3tnojbcmojFVwpF3Ok5YEU7btvPtDPuo66G-93-6pwU0zAJIm2_Or7n1VbbE4wqCl-OaQ1NRSVBzR_SzTrB6z3uNK6DmtqJ7Y5_KzPiVvpe81V2wuPc7VRQ="
# ms_token1 = "MmrwOPItMMBAC0fV0fcH7x4DUGfdMqEBUfhxSHE46dhe_Vj-0_hO-NR99pY6aQpu7AEXuHEFc54T8FgFSx7M6v77C-hlDiF19f1kAykF8YcJFwfutZL6WkkZY65ijWD-69AcfTlrCWdMjph1VEefSO4="

ms_token = os.environ.get(
    "_csgYPYQLIRVbllarU0NFaEzFBLkg8vBxHRu-0NYyQD8Kcl66CDWa9LIc7uE83O1oAIT7nHExABopKP--09jhxxlWDBGWpxsbOmBGQSUa7FPsyWpsaZWaVD98WBlEYi4J74-AJmhx1i6LkTsyT0zbIs=", None
)

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
                # test_user = 'rfaburmese'
                await api.create_sessions(
                    ms_tokens=[ms_token],
                    num_sessions=1,
                    sleep_after=5,
                    headless=False,
                )
                
                ### for database user
                user = api.user(UserInfo.source)
                ### for specific user
                # UserInfo.source_name = input("Enter source name : ")
                # user = api.user(UserInfo.source_name)

                # print(user)

                user_data = await user.info()
                # print(user_data)
                followerCount = user_data["userInfo"]["stats"].get("followerCount")
                followingCount = user_data["userInfo"]["stats"].get("followingCount")
                friendCount = user_data["userInfo"]["stats"].get("friendCount")
                heartCount = user_data["userInfo"]["stats"].get("heartCount")
                post_count = user_data["userInfo"]["stats"].get("videoCount")
                logging.debug(f"User {UserInfo.source} has follower count {followerCount},following count {followingCount}, friend count {friendCount}, heart count {heartCount} ,post count {post_count}.")
                ### for specific user
                # print(f"User {UserInfo.source_name} has follower count {followerCount}, following count {followingCount}, friend count {friendCount}, heart count {heartCount}, post count {post_count}.")

                user_videos = []

                r_data = json.dumps(user_data, indent=4)
                UserInfo.o_data = json.loads(r_data)

                # vcounts = UserInfo.o_data["userInfo"]["stats"].get("videoCount")
                # print(vcounts)

                ####users videos collect#######
                async for video in user.videos(count=30):
                #### for specific user   
                # async for video in user.videos(count=post_count):
                    # print(video.as_dict)
                    user_videos.append(video.as_dict)
                #### collect users data convert json format #######
                v_data = json.dumps(user_videos, indent=4)
                UserInfo.vo_data = json.loads(v_data)

                await UserInfo.insert_video()
                await UserInfo.insert_user()

    async def insert_video():
        with app.app.app_context():
            app.db.create_all()

            for select_data in UserInfo.vo_data:
                ###data collects from user link
                video_id = select_data["id"]
                source_id = UserInfo.o_data["userInfo"]["user"]["id"]
                # source_id = UserInfo.source_name["userInfo"]["user"]["id"]
                video_createtime = select_data["createTime"]
                video_description = str(select_data["desc"])
                video_url = "https://www.tiktok.com/@{}/video/{}".format(
                    UserInfo.source, select_data["id"]
                )
                # video_url = "https://www.tiktok.com/@{}/video/{}".format(
                #     UserInfo.source_name, select_data["id"]
                # )

                if "authorName" not in select_data["music"]:
                    video_author = "orginal"               
                else:
                    video_author = select_data["music"]["authorName"]                 
                
                if "duration" not in select_data["music"]:#["duration"] != "original":
                    video_duration = 0                
                else:
                    video_duration = select_data["music"]["duration"]                

                if "title" not in select_data["music"]: #["title"] != "original":
                    video_music_title = "original"                
                else:
                    video_music_title = select_data["music"]["title"]
                # video_author = select_data["music"]["authorName"]
                # video_duration = select_data["music"]["duration"]
                # video_music_title = select_data["music"]["title"]
                video_collectcount = select_data["stats"]["collectCount"]
                video_commentcount = select_data["stats"]["commentCount"]
                video_diggcount = select_data["stats"]["diggCount"]
                video_playcount = select_data["stats"]["playCount"]
                video_sharecount = select_data["stats"]["shareCount"]
                # print(video_id)

                ###create json object for insert database
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
                                video_id,video_commentcount,video_collectcount,video_playcount,video_sharecount
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
                        content_table = Table("all_content", metadata, autoload_with=engine)

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
                        insert_allcontent = content_table.insert().values(values_to_insert)

                        # Execute the insert statement
                        app.db.session.execute(insert_allcontent)
                        logging.debug("Added content id values : {} for network id 5".format(ids))

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
                        logging.debug(
                            "video data source is updated successful, video id : {},comment : {}, collect : {}, play : {}, share : {}".format(
                                video_id,video_commentcount,video_collectcount,video_playcount,video_sharecount
                            )
                        )
                    except Exception as e:
                        print(f"Error updating data: {e}")
                        app.db.session.rollback()

    async def insert_user():
        with app.app.app_context():
            app.db.create_all()

            source_id = UserInfo.o_data["userInfo"]["user"]["id"]
            user_title = UserInfo.o_data["shareMeta"]["title"]
            user_nickname = UserInfo.o_data["userInfo"]["user"]["nickname"]
            user_uniqueId = UserInfo.o_data["userInfo"]["user"]["uniqueId"]
            user_relation = UserInfo.o_data["userInfo"]["user"]["relation"]
            user_diggcount = UserInfo.o_data["userInfo"]["stats"]["diggCount"]
            user_followercount = UserInfo.o_data["userInfo"]["stats"]["followerCount"]
            user_followingcount = UserInfo.o_data["userInfo"]["stats"]["followingCount"]
            user_friendcount = UserInfo.o_data["userInfo"]["stats"]["friendCount"]
            user_heart = UserInfo.o_data["userInfo"]["stats"]["heart"]
            user_videocount = UserInfo.o_data["userInfo"]["stats"]["videoCount"]
            user_url = "https://www.tiktok.com/@{}".format(user_uniqueId)

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
                    print("user data source {} updated successful.".format(user_uniqueId))
                except Exception as e:
                    print(f"Error updating data: {e}")
                    app.db.session.rollback()

            app.db.session.close()


if __name__ == "__main__":
    # asyncio.run(UserInfo.user_profile_data(all_users="elevenmedia"))
    metadata = MetaData()
    users = Table("tbl_tk_sources", metadata, autoload_with=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # all_users = session.query(users).with_entities(app.TikTokSources.source_name).filter(app.TikTokSources.owner != 1).all()
    all_users = session.query(users).with_entities(app.TikTokSources.source_name).filter(
        (app.TikTokSources.owner == None)& (app.TikTokSources.source_check == True)
    ).all()
    # rand_source = random.sample(all_users, 7)
    # sources = ["".join(user) for user in rand_source]
    # print(sources)

    all_users_list = [user.source_name for user in all_users]    
    sample_size = min(7, len(all_users_list))  # Ensure sample size is not larger than the population
    rand_source = random.sample(all_users_list, sample_size)

    sources = ["".join(user) for user in rand_source]
    print(sources)

    # source_name = input("Enter source name : ")
    # sources = [source_name] #yangonmediagroup #1108 #elevenmedia sayargyi7635 xinhuamyanmar hkwanntout mtnewstoday npnews3 cnimyanmar09 shwemm143 97media_myanmar dvb.burmese people.media1

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete((UserInfo.user_profile_data(sources)))

    # loop.close()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(UserInfo.user_profile_data(sources))
    finally:
        loop.close()
        session.close() 

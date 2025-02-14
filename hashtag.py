from TikTokApi import TikTokApi
import asyncio
import os
import app
import json
from sqlalchemy import update
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import random
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

ms_token = "w0YaBzxVYAiVs4uWHi5DzjEcfWIQ96YS7F7JajFTAqz6xztmvhRZnplBtcCYmHcN4TUWBpjFlw11nuO8oFk1KkEQnvMQ-UgmbMitlwuPsl8ghz4_1zJ47lZh7k8H"
ms_token2 = "tg-gfxTNY1vFk8aX074Bx_fBpvBnQ3n0tiXc1CQsYIVVc0vJkVOeRTUrM62hlDcO_87fLtrP7QSw_8UXofYFoQyeiVB-bhFwrvI4kYhygig5KN1wk-zE3Oisnw3xOxCjUBJ3XiVEPbHKW4i3"
ms_token1 = "HB08JRnY9yq0ZOJNvaxxC_moJSoheE7wfRm1w-dOfiFruZnlkpgGFGSTMX_tHxLZDKS6WRekU4XGZULn3sAPhWzdu0liyU7k-StDAmQeQyzoC2TQLAHAT2tfQJnS"

database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)

class HashInfo:  
    hashkey = []
    async def get_hashtag_videos(hashtag_name):       
        hash_out_data = []
        async with TikTokApi() as api:
            sources = [''.join(hashname) for hashname in hashtag_name] 
            for source in sources:
                HashInfo.hashkey = source
                logger.debug(f"Processing hashtag: {HashInfo.hashkey}")

                await api.create_sessions(ms_tokens=[ms_token, ms_token1, ms_token2], num_sessions=1, sleep_after=3, headless=False)
                tag = api.hashtag(name=HashInfo.hashkey)
                hashtag_data = []
                async for video in tag.videos(count=50):
                    hashtag_data.append(video.as_dict)
                    hash_data = json.dumps(hashtag_data, indent=4)
                    hash_out_data = json.loads(hash_data)

                    with app.app.app_context():
                        app.db.create_all()
                        for hashtag in hash_out_data:
                            hash_name = hashtag_name                             
                            hash_video_id = hashtag['id']                
                            hash_video_createTime = hashtag['createTime']
                            hash_video_duration = hashtag['video']['duration']
                            hash_contents_desc = hashtag['desc']
                            author_id = hashtag['author']['id']
                            author_nickname = hashtag['author']['nickname']
                            author_uniqueId = hashtag['author']['uniqueId']
                            author_diggCount = hashtag['authorStats']['diggCount']
                            author_followerCount = hashtag['authorStats']['followerCount']
                            author_followingCount = hashtag['authorStats']['followingCount']
                            author_friendCount = hashtag['authorStats']['friendCount']
                            author_heartCount = hashtag['authorStats']['heart']
                            author_heart = hashtag['authorStats']['heartCount']
                            author_videoCount = hashtag['authorStats']['videoCount']
                            stats_collectCount = hashtag['stats']['collectCount']
                            stats_commentCount = hashtag['stats']['commentCount']
                            stats_diggCount = hashtag['stats']['diggCount']
                            stats_playCount = hashtag['stats']['playCount']
                            stats_shareCount = hashtag['stats']['shareCount']  
                            user_url = 'https://www.tiktok.com/@{}'.format(author_uniqueId),
                            hash_video_url = 'https://www.tiktok.com/@{}/video/{}'.format(author_nickname, hash_video_id),

                            results = app.db.session.query(app.TikTokHashKey.id).filter(app.TikTokHashKey.hash_name == HashInfo.hashkey).all()
                            ids = [row.id for row in results]
                            if ids:
                                hash_id = int(ids[0])
                            else:
                                logger.warning("No IDs found")

                            hashtag_video = app.TikTokVideosInfo(
                                s_id = hash_id,
                                video_id = hash_video_id,
                                source_id = author_id,
                                video_createtime = datetime.utcfromtimestamp(hash_video_createTime),
                                video_description = hash_contents_desc,
                                video_url = hash_video_url,
                                video_author = author_nickname,
                                video_duration = hash_video_duration,
                                video_collectcount = stats_collectCount,
                                video_commentcount = stats_commentCount,
                                video_diggcount = stats_diggCount,
                                video_playcount = stats_playCount,
                                video_sharecount = stats_shareCount
                            )

                            check_video = app.db.session.query(app.TikTokVideosInfo).filter(app.TikTokVideosInfo.video_id == hash_video_id).first()
                            if check_video is None:
                                try:
                                    app.db.session.add(hashtag_video)
                                    await asyncio.sleep(3) 
                                    app.db.session.commit()
                                    logger.info(f"Added hashtag source: {HashInfo.hashkey}, content id: {hash_video_id}")

                                    content = app.db.session.query(app.TikTokVideosInfo.id).filter(app.TikTokVideosInfo.video_id == hash_video_id).all()
                                    ids = [row.id for row in content]
                                    content_table = Table('all_content', metadata, autoload_with=engine)
                                    columns = content_table.columns.keys()
                                    content_column = columns[1]
                                    network_column = columns[2]
                                    values_to_insert = [{content_column: content_id, network_column: 5} for content_id in ids]
                                    insert_allcontent = content_table.insert().values(values_to_insert)
                                    app.db.session.execute(insert_allcontent)
                                    logger.info(f"Added content id values: {ids} for network id 5")
                                except Exception as e:
                                    logger.error(f"Error updating data: {e}")
                                    app.db.session.rollback()
                            else:
                                update_hashtag_video = update(app.TikTokVideosInfo).where(app.TikTokVideosInfo.video_id == hash_video_id).values(
                                    s_id = hash_id,
                                    video_id = hash_video_id,
                                    source_id = author_id,
                                    video_createtime = datetime.utcfromtimestamp(hash_video_createTime),
                                    video_description = hash_contents_desc,
                                    video_url = hash_video_url,
                                    video_author = author_nickname,
                                    video_duration = hash_video_duration,
                                    video_collectcount = stats_collectCount,
                                    video_commentcount = stats_commentCount,
                                    video_diggcount = stats_diggCount,
                                    video_playcount = stats_playCount,
                                    video_sharecount = stats_shareCount
                                )
                                app.db.session.execute(update_hashtag_video)
                                await asyncio.sleep(3)
                                app.db.session.commit()
                                logger.info(f"Updated hashtag source: {HashInfo.hashkey}, content id: {hash_video_id}")

                            hashtag_info = app.TikTokUsersInfo(
                                source_id = author_id,
                                user_nickname = author_nickname,
                                user_uniqueId = author_uniqueId,
                                user_diggcount = author_diggCount,
                                user_followercount = author_followerCount,
                                user_followingcount = author_followingCount,
                                user_friendcount = author_friendCount,
                                user_heart = author_heartCount,
                                user_videocount = author_videoCount,
                                user_url = user_url,
                            )

                            check = app.db.session.query(app.TikTokUsersInfo).filter(app.TikTokUsersInfo.source_id == author_id).first()
                            if check:
                                update_hashtag_info = update(app.TikTokUsersInfo).where(app.TikTokUsersInfo.source_id == author_id).values(
                                    source_id = author_id,
                                    user_nickname = author_nickname,
                                    user_uniqueId = author_uniqueId,
                                    user_diggcount = author_diggCount,
                                    user_followercount = author_followerCount,
                                    user_followingcount = author_followingCount,
                                    user_friendcount = author_friendCount,
                                    user_heart = author_heartCount,
                                    user_videocount = author_videoCount,
                                    user_url = user_url,
                                )
                                app.db.session.execute(update_hashtag_info)
                                await asyncio.sleep(3)
                                app.db.session.commit()
                                logger.info(f"Updated hashtag: {HashInfo.hashkey}, content create user name: {author_nickname}")
                            else:
                                app.db.session.add(hashtag_info)
                                await asyncio.sleep(3)
                                app.db.session.commit()
                                logger.info(f"Added hashtag: {HashInfo.hashkey}, content create user name: {author_nickname}")

if __name__ == "__main__":
    metadata = MetaData()
    users = Table('tbl_tk_hashtag_sources', metadata, autoload_with=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
                        
    all_hashtag = session.query(users).with_entities(app.TikTokHashKey.hash_name).filter(app.TikTokHashKey.hash_check == True).all()
    rand_hash = random.sample(all_hashtag, 5)
    sources = [''.join(user) for user in rand_hash] 
    logger.debug(f"Random hashtags selected: {sources}")
    
    # source_name = input("Enter hash name: ")
    # sources = ["ပြည်သူ့စစ်မှုထမ်း"]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(HashInfo.get_hashtag_videos(sources))
    loop.close()
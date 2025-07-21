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
import csv
import sys
from TikTokApi.exceptions import EmptyResponseException

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

ms_token = os.environ.get("ms_token", None)
if not ms_token:
    logging.error("No ms_token provided. Please set the ms_token environment variable.")
    sys.exit(1)

database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)

def get_nested(data, *keys):
    """Safely get nested dictionary keys, returns None if any key is missing."""
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return None
        data = data[key]
    return data

class HashInfo:  
    hashkey = []
    async def get_hashtag_videos(hashtag_name):       
        hash_out_data = []
        async with TikTokApi() as api:
            sources = [''.join(hashname) for hashname in hashtag_name] 
            for source in sources:
                HashInfo.hashkey = source
                logging.debug(f"Processing hashtag: {HashInfo.hashkey}")

                await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3, headless=True)
                tag = api.hashtag(name=HashInfo.hashkey)
                hashtag_data = []
                
                try:
                    async for video in tag.videos(count=150):
                        hashtag_data.append(video.as_dict)
                except EmptyResponseException as e:
                    logging.error(f"TikTok returned an empty response for hashtag '{HashInfo.hashkey}': {e}")
                    continue

                if not hashtag_data:
                    logging.warning(f"No videos found for hashtag '{HashInfo.hashkey}'. Skipping database insert.")
                    continue

                hash_data = json.dumps(hashtag_data, indent=4)
                hash_out_data = json.loads(hash_data)

                with app.app.app_context():
                    app.db.create_all()
                    for hashtag in hash_out_data:
                        hash_name = hashtag_name                             
                        hash_video_id = get_nested(hashtag, 'id')
                        hash_video_createTime = get_nested(hashtag, 'createTime')
                        hash_video_duration = get_nested(hashtag, 'video', 'duration')
                        hash_contents_desc = get_nested(hashtag, 'desc')
                        author_id = get_nested(hashtag, 'author', 'id')
                        author_nickname = get_nested(hashtag, 'author', 'nickname')
                        author_uniqueId = get_nested(hashtag, 'author', 'uniqueId')
                        author_diggCount = get_nested(hashtag, 'authorStats', 'diggCount')
                        author_followerCount = get_nested(hashtag, 'authorStats', 'followerCount')
                        author_followingCount = get_nested(hashtag, 'authorStats', 'followingCount')
                        author_friendCount = get_nested(hashtag, 'authorStats', 'friendCount')
                        author_heartCount = get_nested(hashtag, 'authorStats', 'heart')
                        author_videoCount = get_nested(hashtag, 'authorStats', 'videoCount')
                        stats_collectCount = get_nested(hashtag, 'stats', 'collectCount')
                        stats_commentCount = get_nested(hashtag, 'stats', 'commentCount')
                        stats_diggCount = get_nested(hashtag, 'stats', 'diggCount')
                        stats_playCount = get_nested(hashtag, 'stats', 'playCount')
                        stats_shareCount = get_nested(hashtag, 'stats', 'shareCount')  
                        user_url = f'https://www.tiktok.com/@{author_uniqueId}' if author_uniqueId else None
                        hash_video_url = f'https://www.tiktok.com/@{author_nickname}/video/{hash_video_id}' if author_nickname and hash_video_id else None

                        results = app.db.session.query(app.TikTokHashKey.id).filter(app.TikTokHashKey.hash_name == HashInfo.hashkey).all()
                        ids = [row.id for row in results]
                        if ids:
                            hash_id = int(ids[0])
                        else:
                            logging.warning("No IDs found")
                            continue

                        hashtag_video = app.TikTokVideosInfo(
                            s_id = hash_id,
                            video_id = hash_video_id,
                            source_id = author_id,
                            video_createtime = datetime.utcfromtimestamp(hash_video_createTime) if hash_video_createTime else None,
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
                                logging.info(f"Added hashtag source: {HashInfo.hashkey}, content id: {hash_video_id}")

                                content = app.db.session.query(app.TikTokVideosInfo.id).filter(app.TikTokVideosInfo.video_id == hash_video_id).all()
                                ids = [row.id for row in content]
                                content_table = Table('all_content', metadata, autoload_with=engine)
                                columns = content_table.columns.keys()
                                content_column = columns[1]
                                network_column = columns[2]
                                values_to_insert = [{content_column: content_id, network_column: 5} for content_id in ids]
                                insert_allcontent = content_table.insert().values(values_to_insert)
                                app.db.session.execute(insert_allcontent)
                                logging.info(f"Added content id values: {ids} for network id 5")
                            except Exception as e:
                                logging.error(f"Error updating data: {e}")
                                app.db.session.rollback()
                        else:
                            try:
                                update_hashtag_video = update(app.TikTokVideosInfo).where(app.TikTokVideosInfo.video_id == hash_video_id).values(
                                    s_id = hash_id,
                                    video_id = hash_video_id,
                                    source_id = author_id,
                                    video_createtime = datetime.utcfromtimestamp(hash_video_createTime) if hash_video_createTime else None,
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
                                logging.info(f"Updated hashtag source: {HashInfo.hashkey}, content id: {hash_video_id}")
                            except Exception as e:
                                logging.error(f"Error updating data: {e}")
                                app.db.session.rollback()

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
                            try:
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
                                logging.info(f"Updated hashtag: {HashInfo.hashkey}, content create user name: {author_nickname}")
                            except Exception as e:
                                logging.error(f"Error updating data: {e}")
                                app.db.session.rollback()
                        else:
                            try:
                                app.db.session.add(hashtag_info)
                                await asyncio.sleep(3)
                                app.db.session.commit()
                                logging.info(f"Added hashtag: {HashInfo.hashkey}, content create user name: {author_nickname}")
                            except Exception as e:
                                logging.error(f"Error updating data: {e}")
                                app.db.session.rollback()

if __name__ == "__main__":
    metadata = MetaData()
    users = Table('tbl_tk_hashtag_sources', metadata, autoload_with=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
                        
    all_hashtag = session.query(users).with_entities(app.TikTokHashKey.hash_name).filter(app.TikTokHashKey.hash_check == True).all()
    rand_hash = random.sample(all_hashtag, 5)
    sources = [''.join(user) for user in rand_hash] 
    logging.debug(f"Random hashtags selected: {sources}")
    
    try:
        asyncio.run(HashInfo.get_hashtag_videos(sources))
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        session.close()
        sys.exit(0)
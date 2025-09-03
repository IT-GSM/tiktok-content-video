from TikTokApi import TikTokApi
import asyncio
import json
import random
import logging

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import os
from sqlalchemy import update
import app
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

ms_token = os.environ.get("ms_token", None)

# ms_token = os.environ.get(
#     "mwlCTUQCgBh7nhQSEJGC_30BbhrNis1tqIzk5vlEbmlYzoUyNOfc6wHkqreTCG1mmGbrUCLSC3QyvWJ49Ycyw1vfRXbZx9vUiQga1vUId5QQG0WYJVoG2glr71iyIKwVw5VxuCGmRqx_Cg==", None
# )

database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)


def get_nested(data, *keys):
    """Safely get nested dictionary keys, returns None if any key is missing."""
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return None
        data = data[key]
    return data


async def get_comments(video_id, comment_count):
    comment_data = []

    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token],
            num_sessions=1,
            sleep_after=5,
            headless=True,
        )

        vdId = api.video(id="7543806110864559368")
        async for comment in vdId.comments(count=comment_count):
            if comment is not None:
                comment_data.append(comment.as_dict)
                comm_data = json.dumps(comment_data, indent=4)
                comm_out_data = json.loads(comm_data)

                for comments in comm_out_data:
                    comment_id = get_nested(comments, "cid")
                    comment_time = get_nested(comments, "create_time")
                    comment_text = get_nested(comments, "text")
                    comment_diggcount = get_nested(comments, "digg_count")
                    comment_replycount = get_nested(comments, "reply_comment_total")
                    comment_username = get_nested(comments, "user", "unique_id")
                    comment_usernickname = get_nested(comments, "user", "nickname")
                    comment_userId = get_nested(comments, "user", "uid")
                    comment_userurl = f"https://www.tiktok.com/@{comment_username}" if comment_username else None

                    video_comments = app.TiktokCommentsInfo(
                        comment_id=comment_id,
                        video_id=video_id,
                        comment_time=datetime.utcfromtimestamp(comment_time) if comment_time else None,
                        comment_text=comment_text,
                        comment_diggcount=comment_diggcount,
                        comment_replycount=comment_replycount,
                        comment_username=comment_username,
                        comment_usernickname=comment_usernickname,
                        comment_userId=comment_userId,
                        comment_userurl=comment_userurl,
                    )

                result_video = (
                    app.db.session.query(app.TiktokCommentsInfo)
                    .filter(app.TiktokCommentsInfo.comment_id == comment_id)
                    .first()
                )
                if result_video:
                    update_user_comments = (
                        update(app.TiktokCommentsInfo)
                        .where(app.TiktokCommentsInfo.comment_id == comment_id)
                        .values(
                            video_id=video_id,
                            comment_time=datetime.utcfromtimestamp(comment_time),
                            comment_text=comment_text,
                            comment_diggcount=comment_diggcount,
                            comment_replycount=comment_replycount,
                            comment_username=comment_username,
                            comment_usernickname=comment_usernickname,
                            comment_userId=comment_userId,
                            comment_userurl=comment_userurl,
                        )
                    )
                    app.db.session.execute(update_user_comments)
                    await asyncio.sleep(5)
                    app.db.session.commit()
                    logging.debug(
                        f"Updated comment id: {comment_id}, comment text: {comment_text}, video id: {video_id}"
                    )
                else:
                    app.db.session.add(video_comments)
                    await asyncio.sleep(5)
                    app.db.session.commit()
                    logging.debug(
                        f"Added comment id: {comment_id}, comment text: {comment_text}, video id: {video_id}"
                    )
                    # content_id collect from table tiktokvideo info
                    comments = (
                        app.db.session.query(app.TiktokCommentsInfo.id)
                        .filter(app.TiktokCommentsInfo.comment_id == comment_id)
                        .all()
                    )

                    # Extracting the id values from the result
                    ids = [row.id for row in comments]

                    # Reflect the  table from the database
                    comment_table = Table("all_comment", metadata, autoload_with=engine)

                    # Access the columns of the "content" table
                    columns = comment_table.columns.keys()

                    # Print the column names
                    comment_column = columns[1]
                    network_column = columns[2]
                    # print("content_column {},network_column {}".format(ids,5))

                    # Define the values to insert
                    values_to_insert = [
                        {comment_column: comm_id, network_column: 5}
                        for comm_id in ids
                    ]

                    # Create an insert statement
                    insert_allcomment = comment_table.insert().values(values_to_insert)

                    # Execute the insert statement
                    app.db.session.execute(insert_allcomment)
                    logging.debug("Added comment id values : {} for network id 5".format(ids))


async def check_and_crawl_comments():
    with app.app.app_context():
        app.db.create_all()
        # videos = app.TikTokVideosInfo.query.all()

        # Get current UTC time and 24 hours ago
        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        # Filter videos from the last 24 hours
        # videos = (
        #     app.TikTokVideosInfo.query
        #     .filter(app.TikTokVideosInfo.video_createtime >= last_24h)
        #     .order_by(app.TikTokVideosInfo.video_createtime.desc())
        #     .limit(1000)
        #     .all()
        # )

        videos = (
            app.TikTokVideosInfo.query
            .order_by(app.TikTokVideosInfo.id.desc())
            .limit(100)
            .all()
        )
        if len(videos) > 30:
            videos = random.sample(videos, 30)
        for video in videos:
            if video.video_commentcount > 1:
                logging.debug(
                    f"Crawling comments for video id: {video.video_id} with comment count: {video.video_commentcount}"
                )
                await get_comments(video.video_id, video.video_commentcount)


if __name__ == "__main__":
    metadata = MetaData()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_and_crawl_comments())
    loop.close()
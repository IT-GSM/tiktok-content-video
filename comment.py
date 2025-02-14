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
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

ms_token = os.environ.get(
    "AySjOsUhnRUCms09JiJ47wIqlss6EXPeWjdz2otVANAWFCf52sAiJssicwKW4hFt3gI6XSYVe-bdh73KNszJJMYQBT-QOq_7TFMgWFnJM6inN6ATgMQ5",
    None,
)
ms_token2 = os.environ.get(
    "tg-gfxTNY1vFk8aX074Bx_fBpvBnQ3n0tiXc1CQsYIVVc0vJkVOeRTUrM62hlDcO_87fLtrP7QSw_8UXofYFoQyeiVB-bhFwrvI4kYhygig5KN1wk-zE3Oisnw3xOxCjUBJ3XiVEPbHKW4i3",
    None,
)
ms_token1 = os.environ.get(
    "6ZgMsdFEjgHnPcgKASehIfrCLwYKTVDvQJb-x4g8wo1EW8MY4F27aamjmYrYmsacZowsWEwqFxb-Z92K-jBCqM1rCyK8rr96227LVErJD4fLurIAt8tCUy8dY-_pFAa40aqF17ajISbjZOx7",
    None,
)
database_url = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs"
engine = create_engine(database_url)


async def get_comments(video_id, comment_count):
    comment_data = []

    async with TikTokApi() as api:
        await api.create_sessions(
            ms_tokens=[ms_token, ms_token1, ms_token2],
            num_sessions=1,
            sleep_after=5,
            headless=False,
        )

        vdId = api.video(id=video_id)
        async for comment in vdId.comments(count=comment_count):
            if comment is not None:
                comment_data.append(comment.as_dict)
                comm_data = json.dumps(comment_data, indent=4)
                comm_out_data = json.loads(comm_data)

                for comments in comm_out_data:
                    comment_id = comments["cid"]
                    comment_time = comments["create_time"]
                    comment_text = comments["text"]
                    comment_diggcount = comments["digg_count"]
                    comment_replycount = comments["reply_comment_total"]
                    comment_username = comments["user"]["unique_id"]
                    comment_usernickname = comments["user"]["nickname"]
                    comment_userId = comments["user"]["uid"]
                    comment_userurl = "https://www.tiktok.com/@{}".format(
                        comment_username
                    )

                    video_comments = app.TiktokCommentsInfo(
                        comment_id=comment_id,
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


async def check_and_crawl_comments():
    with app.app.app_context():
        app.db.create_all()
        videos = app.TikTokVideosInfo.query.all()
        if len(videos) > 5:
            videos = random.sample(videos, 5)
        for video in videos:
            if video.video_commentcount > 1:
                logging.debug(
                    f"Crawling comments for video id: {video.video_id} with comment count: {video.video_commentcount}"
                )
                await get_comments(video.video_id, video.video_commentcount)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_and_crawl_comments())
    loop.close()
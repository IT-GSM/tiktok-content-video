from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import datetime
from sqlalchemy import TIMESTAMP, DateTime
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import BIGINT

app = Flask(__name__)
#app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:admin@localhost:5432/dbtiktok"
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://fbs:yah7WUy1Oi8G@192.168.11.202:5432/fbs" #yah7WUy1Oi8G

db = SQLAlchemy(app)
migrate = Migrate(app, db)

class TikTokSources(db.Model):
    __tablename__ = 'tbl_tk_sources'

    id = db.Column(db.Integer, primary_key=True)
    #source_id = db.Column(BIGINT)
    source_name = db.Column(db.String())
    owner = db.Column(db.Integer)
    source_check = db.Column(db.Boolean,unique=False, default=True)

class TikTokHashKey(db.Model):
    __tablename__ = 'tbl_tk_hashtag_sources'

    id = db.Column(db.Integer, primary_key=True)
    #source_id = db.Column(BIGINT)
    hash_name = db.Column(db.String())
    hash_check = db.Column(db.Boolean,unique=False, default=True)
    hash_service = db.Column(db.String())
    
class TikTokUsersInfo(db.Model):
    __tablename__ = 'tbl_tk_users_info'

    id = db.Column(db.Integer,primary_key=True)
    source_id = db.Column(BIGINT)
    user_title = db.Column(db.String())
    user_nickname = db.Column(db.String())
    user_uniqueId = db.Column(db.String())
    user_relation = db.Column(db.String())

    user_diggcount = db.Column(db.Integer)
    user_followercount = db.Column(db.Integer)
    user_followingcount = db.Column(db.Integer)
    user_friendcount = db.Column(db.Integer)
    user_heart = db.Column(db.Integer)
    # user_heartCount = db.Column(db.Integer)
    user_videocount = db.Column(db.Integer)
    user_url = db.Column(db.String())

class TikTokVideosInfo(db.Model):
    __tablename__ = 'tbl_tk_videos_info'
     
    id = db.Column(db.Integer,primary_key=True)
    s_id = db.Column(db.Integer)
    video_id = db.Column(BIGINT)
    source_id = db.Column(BIGINT)
    video_createtime = db.Column(TIMESTAMP(timezone=True))
    video_description = db.Column(db.String())
    video_url = db.Column(db.String())

    video_author = db.Column(db.String())
    video_duration = db.Column(db.String())
    video_music_title = db.Column(db.String())
    
    video_collectcount = db.Column(db.Integer)
    video_commentcount = db.Column(db.Integer)
    video_diggcount = db.Column(db.Integer)
    video_playcount = db.Column(db.Integer)
    video_sharecount = db.Column(db.Integer)

class TiktokCommentsInfo(db.Model):
    __tablename__ = 'tbl_tk_comments_info'

    id = db.Column(db.Integer, primary_key=True)    
    s_id = db.Column(db.Integer)
    comment_id = db.Column(BIGINT)
    video_id = db.Column(BIGINT)
    # video_url = db.Column(db.String())
    comment_time = db.Column(TIMESTAMP(timezone=True))
    comment_text = db.Column(db.String())
    comment_diggcount = db.Column(db.Integer)
    comment_replycount = db.Column(db.Integer)    
    comment_username = db.Column(db.String())
    comment_usernickname = db.Column(db.String())
    comment_userId = db.Column(db.String())
    comment_userurl = db.Column(db.String())

# class TikTokHashUserInfo(db.Model):
#     __tablename__ = 'tbl_tk_hashtag_user_info'
     
#     id = db.Column(db.Integer,primary_key=True)
#     s_id = db.Column(db.Integer)

#     hash_name = db.Column(db.String())
#     hash_video_id = db.Column(BIGINT)    
#     # hash_video_url = db.Column(db.String())
#     # hash_video_createTime = db.Column(db.String())    
#     # hash_video_duration = db.Column(db.String())    
#     # hash_contents_desc = db.Column(db.String())

#     author_id = db.Column(BIGINT)
#     author_nickname = db.Column(db.String())
#     author_uniqueId = db.Column(db.String())
#     author_diggCount = db.Column(db.Integer)
#     author_followerCount = db.Column(db.Integer)
#     author_followingCount = db.Column(db.Integer)
#     author_friendCount = db.Column(db.Integer)
#     author_heartCount = db.Column(db.Integer)
#     author_heart = db.Column(db.Integer)
#     author_videoCount = db.Column(db.Integer)

#     user_url = db.Column(db.String())

    # stats_collectCount = db.Column(db.Integer)
    # stats_commentCount = db.Column(db.Integer)
    # stats_diggCount = db.Column(db.Integer)
    # stats_playCount = db.Column(db.Integer)
    # stats_shareCount = db.Column(db.Integer)


# class TkUsersInfoModel(db.Model):
#     __tablename__ = 'tbl_tk_users_data'

#     id = db.Column(db.Integer, primary_key=True)
#     userinfo = db.Column(JSON)
#     uservideo = db.Column(JSON)
#     commentvideo = db.Column(JSON)


# class TkVideosComment(db.Model):
#     __tablename__ = 'tbl_tk_videos_comments'

#     id = db.Column(db.Integer, primary_key=True)
#     video_id = db.Column(BIGINT)
#     video_comments = db.Column(JSON)


from pymongo import MongoClient
from config import MONGO_URI


# MongoDB setup
mongo = MongoClient(MONGO_URI)
db = mongo["sharing_bot"]
files_col = db["files"]
tmdb_col = db["tmdb"]
tokens_col = db["tokens"]
auth_users_col = db["auth_users"]
allowed_channels_col = db["allowed_channels"]
users_col = db["users"]



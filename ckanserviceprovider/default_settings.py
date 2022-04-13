import os

DEBUG = False
TESTING = False
SECRET_KEY = "please_replace_me"
USERNAME = "admin"
PASSWORD = "pass"

# database

SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL", "sqlite://")
SQLALCHEMY_ECHO = False

# webserver host and port

HOST = "0.0.0.0"
PORT = 8000

# logging

LOG_FILE = "/tmp/ckan_service.log"
STDERR = True

# project configuration
NAME = "service"

# days to keep when clearing
KEEP_JOBS_AGE = 14

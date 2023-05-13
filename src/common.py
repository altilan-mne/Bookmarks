"""Common data for all modules of the project.

"""
from collections import namedtuple

MenuItem = namedtuple('MenuItem', 'descr call')  # namedtuple for menu items, avoid dict and index access
Field = namedtuple('Field', 'name text')  # namedtuple for bookmark's field

# ---- common constants ----
VERSION = '3.1 (mongod standalone data format)'
FILL_HEADER = '-'  #: a character to fill header's line
VALID_CHARS = "_-. /"  #: valid special characters in the names of bookmarks ot trees
URL_FIELDS = ['name', 'url', 'icon', 'keywords']  #: enabled url fields to modify
FOLDER_FIELDS = ['name']  #: enabled url fields to modify

# ---- mongo DB constants ----
MONGO_SEEDS = ['localhost:27018', 'localhost:27019', 'localhost:27020']  # local MongoDB seeds
SERVER_SELECTION_TIMEOUT_MS = 500  # default timeout for server connection, in milliseconds

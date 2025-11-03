import os
from .env_config import *


# Define the application directory

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


# Application threads. A common general assumption is
# using 2 per available processor cores - to handle
# incoming requests using one and performing background
# operations using the other.
THREADS_PER_PAGE = 2

#Enable protection against *Cross-site Request Forgery (CSRF)*
CSRF_ENABLED = True

# Debug Flag
DEBUG = True

LOGFILE_PATH = u''

CACHE_TYPE = 'simple'
CACHE_DEFAULT_TIMEOUT = 300
# flake8: noqa
import sys

try:
    from fastapi_users_db_opensearch import OpenSearchUserDatabase
except:
    sys.exit(1)

sys.exit(0)

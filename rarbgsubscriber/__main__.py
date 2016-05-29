import sys
import json

from .rarbg import RarbgSubscriber


assert len(sys.argv) == 2
CONF_DICT = None
CONF_PATH = sys.argv[1]
with open(CONF_PATH, "rb") as fp:
    CONF = fp.read()
    CONF_DICT = json.loads(CONF)

RARBGSUB = RarbgSubscriber(CONF_DICT)
RARBGSUB.start()

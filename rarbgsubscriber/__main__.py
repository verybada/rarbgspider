import sys
import json

from .rarbg import RarbgSubscriber


assert len(sys.argv) == 2
conf_dict = None
conf_path = sys.argv[1]
with open(conf_path, "rb") as fp:
    conf = fp.read()
    conf_dict = json.loads(conf)

r = RarbgSubscriber(conf_dict)
r.start()

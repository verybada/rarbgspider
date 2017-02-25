import os
import json
import argparse
import sqlite3
import logging
import logging.config
import logging.handlers
from collections import defaultdict

from .rarbg import RarbgDaemon
from .pool import (MoviePool, MovieInfo)
from .handler import HandlerManager
from .filter import Filter


def setting_logger(workspace, debug):
    log_dict = {
        "version": 1,
        "disable_existing_loggers": not debug,
        "root": {
            "level": "NOTSET",
            "handlers": ["console", "file"]
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "precise",
            },
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "DEBUG",
                "filename": "%s/rarbgsubscriber.log" % workspace,
                "formatter": "detail",
                "when": "D",
                "backupCount": 7
            }
        },
        "formatters": {
            "precise": {
                "format": "%(asctime)s - %(message)s"
            },
            "detail": {
                "format": ("%(asctime)s %(filename)s:%(funcName)s:"
                           "%(lineno)d - %(message)s")
            }
        },
    }

    logging.config.dictConfig(log_dict)


parser = argparse.ArgumentParser()
parser.add_argument('--conf', required=True,
                    help='configure of rarbg subscriber')
parser.add_argument('-f', '--foreground', action='store_true', help='run in foreground')
args = parser.parse_args()

with open(args.conf, 'rb') as fp:
    data = fp.read()
    conf = defaultdict(dict)
    conf.update(json.loads(data))

    general_conf = conf['general']
    workspace = general_conf.get('workspace', 'RarbgSubscriber')
    pidfile = os.path.join(workspace, 'pid')
    interval = general_conf.get('interval')
    db_path = os.path.join(workspace, 'pool')
    db = sqlite3.connect(db_path)
    pool = MoviePool(db)
    debug = general_conf.get('debug', False)
    filter_ = Filter(conf['filter']) if conf['filter'] else None
    handler_mgr = HandlerManager(conf['handlers']) \
        if conf['handler_mgr'] else None

    setting_logger(workspace, debug)
    daemon = RarbgDaemon(pidfile, pool, filter_=filter_, handler_mgr=handler_mgr,
                         interval=interval)
    if args.foreground:
        daemon.run()
    else:
        daemon.start()

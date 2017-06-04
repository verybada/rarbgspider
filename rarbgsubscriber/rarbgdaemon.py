import sys
import json
import time
import logging
import sqlite3
import argparse

from .daemon import Daemon
from .rarbgsubscriber import RarbgSubscriber


class RarbgDaemon(Daemon):
    def __init__(self, config, database):
        super(RarbgDaemon, self).__init__(pidfile='rarbg.pid')
        with open(config, 'r') as fp:
            data = fp.read()
            self._conf = json.loads(data)
        self._conn = sqlite3.connect(database, check_same_thread=False)
        self._rarbg = RarbgSubscriber(self._conf, self._conn)
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        subscriber = RarbgSubscriber(self._conf, self._conn)
        subscriber.start()
        while not self._stop:
            time.sleep(1)
        subscriber.stop()
        subscriber.join()


def setup_logger(workspace, verbose=False):
    log_dict = {
        "version": 1,
        "disable_existing_loggers": False,
        "propagate": True,
        "loggers": {
            '': {
                "level": logging.DEBUG if verbose else logging.INFO,
                "handlers": ["console", "file"]
            },
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conf', required=True)
    parser.add_argument('--log', required=True)
    parser.add_argument('--db', required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    setup_logger(args.log, args.verbose)
    daemon = RarbgDaemon(args.conf, args.db)
    daemon.start()


if __name__ == '__main__':
    sys.exit(main())

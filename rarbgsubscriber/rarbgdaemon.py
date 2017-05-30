import sys
import json
import time
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conf', required=True)
    parser.add_argument('--db', required=True)
    args = parser.parse_args()
    daemon = RarbgDaemon(args.conf, args.db)
    daemon.start()


if __name__ == '__main__':
    sys.exit(main())

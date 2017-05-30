import time
import sqlite3
import argparse

from .daemon import Daemon
from .rarbgsubscriber import RarbgSubscriber


class RarbgDaemon(Daemon):
    def __init__(self, config, database):
        super(RarbgDaemon, self).__init__()
        with open(config, 'r') as fp:
            self._conf = fp.read()
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
    parser.add_argument('--conf')
    parser.add_argument('--db')
    args = parser.parse_args()
    daemon = RarbgDaemon(args.conf, args.db)
    daemon.start()

import time
import logging
import itertools
from threading import Thread

from rarbgapi import RarbgAPI

from .pool import TorrentPool
from .handler import HandlerManager


class RarbgSubscriber(Thread):
    '''
    {
        'channel': {
            '44': {
                # search torrent name has foo or bar in category 44
                'title': ['foo', 'bar']
            },
            '18': {
                # no filter
            }
        },
        'interval': 3600,
        'handlers': {
            ...
        }
    }
    '''
    def __init__(self, conf, conn):
        super(RarbgSubscriber, self).__init__()
        self._conf = conf
        self._pool = TorrentPool(conn)
        self._apis = RarbgAPI()
        self._stop = False
        handlers = self._conf['handlers']
        self._handlers = HandlerManager(handlers)
        self._logging = logging.getLogger(__name__)

    def stop(self):
        self._stop = True

    def _query(self, category, query):
        for torrent in self._apis.search(limit=100,
                                         search_string=query,
                                         category=category):
            yield torrent

    def run(self):
        interval = self._conf.get('interval')
        while not self._stop:
            channels = self._conf['channel']
            for category, filters in channels.iteritems():
                search_string = filters.get('title')
                if not isinstance(search_string, list):
                    search_string = [search_string, ]
                gens = list()
                for string in search_string:
                    gen = self._query(category, string)
                    gens.append(gen)

                for torrent in itertools.chain(*gens):
                    if self._pool.query(torrent.filename, torrent.category):
                        break
                    self._handlers.register(torrent)
                    self._pool.insert(torrent)

            self._handlers.submit()
            self._pool.commit()
            if not interval:
                self.stop()
                continue
            time.sleep(interval)

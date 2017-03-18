import re
import logging
import time
import datetime
import urlparse

import requests
from bs4 import BeautifulSoup
from guessit import guessit

from .daemon import Daemon
from .pool import MovieInfo


LOG = logging.getLogger(__name__)


class RarbgClient(object):  # pylint: disable=too-few-public-methods
    def __init__(self):
        self._cookie = ''
        self._host = 'http://rarbg.to'

    def conn(self, uri, query=None):
        url = urlparse.urljoin(self._host, uri)
        header = {
            'Cookie': self._cookie,
            'Host': 'rarbg.to',
        }

        sess = requests.Session()
        resp = sess.get(url, params=query, headers=header)
        resp.raise_for_status()
        return resp


# pylint: disable=invalid-name
class TorrentListPage(object):  # pylint: disable=too-few-public-methods
    def __init__(self, host, resp):
        self._host = host
        self._soup = BeautifulSoup(resp.content, 'lxml')

    def _get_imdb(self, tag):  # pylint: disable=no-self-use
        if not tag:
            return 0.0

        ptr = tag.text.find('IMDB:')
        if ptr < 0:
            return 0.0

        text = tag.text[ptr+len('IMDB: '):]
        return float(text.split('/')[0])

    def _get_genres(self, tag):  # pylint: disable=no-self-use

        if not tag:
            return "UNKNOWN"

        genres = re.match('(.+)IMDB', tag.text)
        if not genres:
            return "UNKNOWN"
        temp_genres = genres.group(1)
        temp_genress = temp_genres.replace(",", "<br>")
        return temp_genress

    def _parse(self, tr):
        result = dict()
        _, info_tag, _, size_tag, _, _, _, _ = tr.find_all('td')
        imdb_tag = info_tag.find('span', style=re.compile('color*'))
        result['screen_size'] = "UNKNOWN"
        result['href'] = urlparse.urljoin(self._host, info_tag.a['href'])
        result['imdb'] = self._get_imdb(imdb_tag)
        result['genres'] = self._get_genres(imdb_tag)
        result['size'] = size_tag.text
        image_parser = re.match('.+src=\\\\\'(.+)\\\\\' ',
                                info_tag.a['onmouseover'])
        result['image'] = urlparse.urljoin('http:', image_parser.group(1))
        result.update(guessit(info_tag.a['title']))
        return RarbgTorrent(result)

    def __iter__(self):
        lista2t_table = self._soup.find('table', class_='lista2t')
        assert lista2t_table
        for tr in lista2t_table.find_all('tr', class_='lista2'):
            try:
                yield self._parse(tr)
            except Exception as exp:  # pylint: disable=broad-except
                LOG.exception("Parsing error. tr %s, exc %s", tr, exp)
                continue


# pylint: disable=too-many-instance-attributes
class RarbgTorrent(RarbgClient, dict):
    def __init__(self, raw):
        super(RarbgTorrent, self).__init__()
        self._raw = raw
        self._page_href = raw['href']
        self._page_soup = None

        self.title = raw['title']
        self.year = raw.get('year', 'UNKNOWN')
        self.resolution = raw['screen_size']
        self.format = raw.get('format', 'UNKNOWN')
        self.size = raw['size']
        self.video_codec = raw.get('video_codec')
        self.audio_codec = raw.get('audio_codec')
        self.imdb = raw['imdb']
        self.genres = raw.get('genres')

    def __str__(self):
        return "%s.%s.%s.%s.%s.%s.%s" % (self.title, self.year,
                                         self.resolution, self.format,
                                         self.video_codec, self.audio_codec,
                                         self.size)

    @property
    def href(self):
        if not self._page_soup:
            resp = self.conn(self._page_href)
            self._page_soup = BeautifulSoup(resp.content, 'lxml')

        a = self._page_soup.find('a', href=re.compile('download.php'))
        return urlparse.urljoin(self._host, a["href"])

    @property
    def cover(self):
        if not self._page_soup:
            resp = self.conn(self._page_href)
            self._page_soup = BeautifulSoup(resp.content, 'lxml')

        img = self._page_soup.find("img", itemprop="image")
        return urlparse.urljoin(self._host, img['src'])


class RarbgPager(RarbgClient):
    def __init__(self, category=None, search=None,
                 start_page=1, end_page=None):
        super(RarbgPager, self).__init__()
        self._uri = "torrents.php"
        self._category = category
        self._search = search
        self._current_index = start_page
        self._max_index = end_page
        self._page = None

    @property
    def page(self):
        return self._page

    def _query(self):
        query = {
            'page': self._current_index
        }
        if self._category:
            query['category'] = self._category

        if self._search:
            query['search'] = self._search

        return self.conn(self._uri, query=query)

    def _get_next_index(self, resp):
        soup = BeautifulSoup(resp.content, 'lxml')
        tag = soup.find('div', id='pager_links')
        assert tag
        b_tag = tag.find('b')
        if not b_tag:
            self._max_index = self._current_index
            return self._max_index
        value = int(b_tag.text)
        next_tag = tag.find('a', title='next page')
        if not next_tag:
            if self._current_index >= value:
                self._max_index = self._current_index
        return value+1

    def __iter__(self):
        return self

    def next(self):
        if self._current_index == self._max_index:
            raise StopIteration()

        LOG.debug("Page %s", self._current_index)
        resp = self._query()
        self._page = TorrentListPage(self._host, resp)
        self._current_index = self._get_next_index(resp)
        return self._page


class RarbgSubscriber(object):
    # pylint: disable=redefined-outer-name
    def __init__(self, pool, filter_=None, handler_mgr=None, debug=False):
        self.debug = debug
        self._pool = pool
        self._filter = filter_
        self._handler_mgr = handler_mgr

    def _convert_to_movie_info(self, torrent):  # pylint: disable=no-self-use
        info = None
        try:
            info = MovieInfo(torrent._raw)  # pylint: disable=protected-access
            info.href = torrent.href
        except Exception:  # pylint: disable=broad-except
            LOG.warn("Incomplete torrent %s", torrent)
        return info

    def run(self):
        stop = False
        pager = RarbgPager(category=44)
        for page in pager:
            if stop:
                break

            for torrent in page:
                if not torrent:
                    continue

                if not self._filter.filter(torrent):
                    LOG.debug("Skip %s", torrent)
                    continue

                info = self._convert_to_movie_info(torrent)
                if not info:
                    continue

                if self._pool.find(info.href):
                    # FIXME: ugly
                    stop = True
                    LOG.info("duplication torrent, stop")
                    break

                LOG.info("New torrent %s", info)
                self._pool.insert(info)
                self._handler_mgr.register(info)
            # TODO: rate control
            time.sleep(5)

        self._handler_mgr.submit()


class RarbgDaemon(Daemon):
    def __init__(self, pidfile, pool,
                 filter_=None, handler_mgr=None, interval=None):
        self._filter = filter_
        self._handler_mgr = handler_mgr
        self._interval = interval
        self._pool = pool
        super(RarbgDaemon, self).__init__(pidfile)

    def start(self):
        super(RarbgDaemon, self).start()

    def run(self):
        while self.daemon_alive:
            try:
                rarbg = RarbgSubscriber(self._pool, self._filter,
                                        self._handler_mgr)
                rarbg.run()
            except Exception as exp:
                LOG.exception(exp)
                self.daemon_alive = False
                break

            if not self._interval:
                self.daemon_alive = False
                break

            t = time.time() + self._interval
            next_time = datetime.datetime.fromtimestamp(t)
            LOG.info("Next scan at %s", next_time)
            time.sleep(self._interval)

        LOG.debug("rarbg daemon stopped")

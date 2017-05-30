from rarbgapi import Torrent, RarbgAPI


def convert_category(category_str):
    # TODO
    mapping = {
        'Movies/x264': RarbgAPI.CATEGORY_MOVIE_H264,
        'Movies/x264/1080': RarbgAPI.CATEGORY_MOVIE_H264_1080P,
        'Movies/x264/720': RarbgAPI.CATEGORY_MOVIE_H264_720P,
    }
    return mapping[category_str]


class TorrentPool(object):
    def __init__(self, connection):
        self._conn = connection
        self._create_table()

    def _create_table(self):
        c = self._conn.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS '
                  'torrents(filename TEXT, category INT,magnet TEXT)')
        c.execute('CREATE INDEX IF NOT EXISTS filename_category ON '
                  'torrents(filename,category)')

    def insert(self, torrent):
        category = convert_category(torrent.category)
        c = self._conn.cursor()
        c.execute('INSERT OR REPLACE INTO torrents(filename, category, magnet)'
                  ' VALUES(?,?,?)', (torrent.filename,
                                     category, torrent.download))

    def query(self, filename, category):
        category = convert_category(category)
        c = self._conn.cursor()
        result = list()
        for raw in c.execute('SELECT * FROM torrents '
                             'WHERE filename=? and category=?', (filename,
                                                                 category)):
            torrent = Torrent({
                'filename': raw[0],
                'category': raw[1],
                'download': raw[2]
            })
            result.append(torrent)
        return result

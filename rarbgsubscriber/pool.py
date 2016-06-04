class MovieInfo(dict):  # pylint: disable=too-many-instance-attributes
    def __init__(self, raw):
        super(MovieInfo, self).__init__(raw)
        self._raw = raw
        print("raw==%s" %raw)
        self.title = raw['title']
        self.year = raw.get('year', 'UNKNOWN')
        self.resolution = raw['screen_size']
        self.format = raw.get('format', 'UNKNOWN')
        self.href = raw['href']
        self.size = raw['size']
        self.video_codec = raw.get('video_codec')
        self.audio_codec = raw.get('audio_codec')
        self.imdb = raw['imdb']
        self.image = raw.get('image')
        self.Genres = raw.get('Genres', 'UNKNOWN')

    def __str__(self):
        return "%s.%s.%s.%s.%s.%s.%s" % (self.title, self.year,
                                         self.resolution, self.format,
                                         self.video_codec, self.audio_codec,
                                         self.size)


class MoviePool(object):
    def __init__(self, db_connection):
        self._con = db_connection
        self._create_table()

    def _create_table(self):
        c = self._con.cursor()
        c.execute("PRAGMA foreign_keys = ON")
        c.execute("""CREATE TABLE IF NOT EXISTS pool(
                    image TEXT,
				    title TEXT,
                    year TEXT,
                    PRIMARY KEY(title, year))""")

        c.execute("""CREATE TABLE IF NOT EXISTS detail(
                    title TEXT,
                    year TEXT,
                    resolution TEXT,
                    format TEXT,
                    href TEXT,
                    video_codec TEXT,
                    audio_codec TEXT,
                    imdb REAL,
                    size INTEGER,
                    FOREIGN KEY (title, year) REFERENCES pool(title, year),
                    PRIMARY KEY (href))""")
        c.execute("""CREATE INDEX IF NOT EXISTS detail_title_year
                  ON detail(title, year)""")
        c.execute("""CREATE INDEX IF NOT EXISTS detail_resolution
                  ON detail(resolution)""")
        c.execute("""CREATE INDEX IF NOT EXISTS detail_size ON detail(size)""")
        c.execute("""CREATE INDEX IF NOT EXISTS detail_imdb ON detail(imdb)""")
        self._con.commit()

    def insert(self, value):
        assert isinstance(value, MovieInfo)

        c = self._con.cursor()
        c.execute("INSERT OR IGNORE INTO pool(title, year) VALUES(?,?)",
                  (value.title, value.year))
        c.execute("""INSERT OR REPLACE INTO detail(
                    title, year, resolution, format, href, video_codec,
                    audio_codec, imdb, size) VALUES(?,?,?,?,?,?,?,?,?)""",
                  (value.title, value.year, value.resolution, value.format,
                   value.href, value.video_codec, value.audio_codec,
                   value.imdb, value.size))

    def _result_to_obj(self, result):  # pylint: disable=no-self-use
        info_dict = {
            'title': result[0],
            'year': result[1],
            'screen_size': result[2],
            'format': result[3],
            'href': result[4],
            'video_codec': result[5],
            'audio_codec': result[6],
            'imdb': result[7],
            'size': result[8]
        }
        return MovieInfo(info_dict)

    def find(self, href):
        c = self._con.cursor()
        c.execute("SELECT * FROM detail WHERE href==?", (href,))
        result = c.fetchone()
        if not result:
            return None
        return self._result_to_obj(result)

    def query(self, **kwargs):
        q = ""
        for k, v in kwargs:
            if q:
                q += " AND "
            q += "%s == %s" % (k, v)

        c = self._con.cursor()
        c.execute("SELECT * FROM detail %s" % q)
        while True:
            result = c.fetchone()
            if not result:
                break

            yield self._result_to_obj(result)

    def remove(self, value):  # pylint: disable=no-self-use
        assert isinstance(value, MovieInfo)
        # First, delte from detail
        # c = self._con.cursor()
        # c.execute("DELETE FROM ")
        # Second, if no one reference, delete from pool

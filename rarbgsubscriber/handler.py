import os
import smtplib
import logging
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class HandlerManager(object):
    '''
    {
        'html': {
            'output': '/foo/bar/output.html'
        },
        'email': {
            'host': 'xxx',
            'port': xxx,
            'account': 'xxx',
            'password': 'xxx',
            'to': ['a@mail.com', 'b@mail.com']
        }
    }
    '''
    def __init__(self, conf):
        self._log = logging.getLogger(__name__)
        self._conf = conf
        self._handlers = []
        self._parse_conf()

    def _parse_conf(self):
        for name, value_dict in self._conf.iteritems():
            if name not in ['email', 'html']:
                self._log.debug("Unknow handler %s", name)
                continue

            handler = None
            if name == 'email':
                handler = EmailHandler(**value_dict)
            elif name == 'html':
                handler = HtmlHandler(**value_dict)
            self._log.debug('append %s handler', handler)
            self._handlers.append(handler)

    def register(self, torrent):
        for handler in self._handlers:
            handler.register(torrent)

    def submit(self):
        for handler in self._handlers:
            handler.submit()


class Handler(object):
    def __init__(self):
        pass

    def init(self):
        pass

    def register(self, torrent):
        pass

    def submit(self):
        pass


class HtmlHandler(Handler):
    def __init__(self, output="RarbgSubscriber.html"):
        super(HtmlHandler, self).__init__()
        self._torrents = list()
        self.output = output

    def _info_to_html(self):
        html = "<html>"
        html += "<table border==1>"
        html += """
            <tr>
                <th bgcolor="#b8b894">Title</th>
                <th bgcolor="#b8b894">Category</th>
                <th bgcolor="#b8b894">Link</th>
            </tr>"""
        for torrent in self._torrents:
            html += """
                <tr>
                    <td align=center>%s</td>
                    <td align=center>%s</td>
                    <td align=center>%s</td>
                </tr>
            """ % (torrent.filename, torrent.category, torrent.download)
        html += "</table>"
        html += "</html>"
        return html

    def _reset(self):
        self._torrents = list()

    def register(self, torrent):
        self._torrents.append(torrent)

    def submit(self):
        # pylint: disable=invalid-name
        if os.path.exists(self.output):
            stat = os.stat(self.output)
            ts = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d')
            new_path = "%s.%s" % (self.output, ts)
            os.rename(self.output, new_path)

        with open(self.output, "w+") as fp:
            html = self._info_to_html()
            fp.write(html)
        self._reset()


class EmailHandler(HtmlHandler):
    # pylint: disable=too-many-arguments
    def __init__(self, host=None, port=None,
                 account=None, password=None, from_=None, to=None):
        assert host
        assert port
        assert account
        assert password
        assert to
        assert from_

        super(EmailHandler, self).__init__()
        self._host = host
        self._port = port
        self._account = account
        self._password = password
        self._from = from_
        self._to = to
        self._log = logging.getLogger(__name__)

    def submit(self):
        today = date.today()
        if not self._torrents:
            self._log.info("%s without any updated torrent", today)
            return
        outer = MIMEMultipart()
        outer['Subject'] = "%s RARBG updated torrents" % today
        outer['From'] = self._from
        outer['To'] = ','.join(self._to)
        html = self._info_to_html()
        msg = MIMEText(html, 'html')
        outer.attach(msg)

        s = smtplib.SMTP(self._host, self._port)
        s.starttls()
        s.login(self._account, self._password)
        s.sendmail(self._from, self._to, outer.as_string())
        self._log.info("%s has %d new torrents, sending mail to %s",
                       today, len(self._torrents), self._to)
        self._reset()

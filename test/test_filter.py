import pytest
import logging
import sys

from rarbgsubscriber.filter import *
from rarbgsubscriber.pool import *

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
def get_filter_obj(screen_size="1080p", format_="BluRay", year=[0, 0],
                   video="h264", audio="DTS"):
    conf = {
        "resolution": screen_size,
        "format": format_,
        "year": year,
        "audio_codec": audio,
        "video_codec": video,
    }
    return Filter(conf)


def get_movie_info(title="AGoodMovie", year=1970, screen_size="1080p",
                   format_="BluRay", video="h264", audio="dts",
                   imdb=10, size="10GB"):
    dict_ = {
        'title': title,
        'year': year,
        'screen_size': screen_size,
        'format': format_,
        'href': 'href',
        'size': size,
        'video_codec': video,
        'audio_codec': audio,
        'imdb': imdb,
    }
    return MovieInfo(dict_)


def test_filter_invalid_conf():
    conf = {
        "invalid": "invalid"
    }
    f = Filter(conf)
    mi = get_movie_info()
    with pytest.raises(AssertionError):
        f.filter(mi)


def test_filter_not_set():
    f = Filter(None)
    mi = get_movie_info()
    assert f.filter(mi)


def test_filter_year():
    f = get_filter_obj()
    mi = get_movie_info(year=1970)
    assert f.filter(mi)

    f = get_filter_obj(year=[1970, 0])
    mi = get_movie_info(year=1969)
    assert not f.filter(mi)

    f = get_filter_obj(year=[1970, 1999])
    mi = get_movie_info(year=2000)
    assert not f.filter(mi)

    mi = get_movie_info(year=1988)
    assert f.filter(mi)


def test_filter_screen_size():
    f = get_filter_obj(screen_size="1080p")
    mi = get_movie_info(screen_size="1080p")
    assert f.filter(mi)

    mi = get_movie_info(screen_size="720p")
    assert not f.filter(mi)

    f = get_filter_obj(screen_size=["1080p", "720p"])
    assert f.filter(mi)
    mi = get_movie_info(screen_size="1080p")
    assert f.filter(mi)
    mi = get_movie_info(screen_size="540p")
    assert not f.filter(mi)


def test_filter_format():
    f = get_filter_obj(format_="BluRay")
    mi = get_movie_info(format_="BluRay")
    assert f.filter(mi)

    mi = get_movie_info(format_="BDRIP")
    assert not f.filter(mi)

    f = get_filter_obj(format_=["BluRay", "4K"])
    mi = get_movie_info(format_="BluRay")
    assert f.filter(mi)
    mi = get_movie_info(format_="4K")
    assert f.filter(mi)
    mi = get_movie_info(format_="8K")
    assert not f.filter(mi)

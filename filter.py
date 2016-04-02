import logging

LOG = logging.getLogger(__name__)

class Filter(object):
    def __init__(self, conf):
        self._conf = conf

    def filter(self, movie_info):
        '''
        return False if not pass the filter
        '''
        if not self._conf:
            return True

        for key, value in self._conf.iteritems():
            info_value = getattr(movie_info, key, None)
            LOG.debug("Filter %s expected %s, current %s",
                      key, value, info_value)
            if not info_value:
                return False

            if key == "year":
                min_, max_ = value
                if min_ != 0 and info_value < min_:
                    return False

                if max_ != 0 and info_value > max:
                    return False
            else:
                if info_value != value:
                    return False

        return True


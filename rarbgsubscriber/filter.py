import logging

LOG = logging.getLogger()

# pylint: disable=too-few-public-methods,unused-argument,no-self-use
class Filter(object):
    def __init__(self, conf):
        self._conf = conf

    def _filter_max_min(self, key, value, info_value):
        min_, max_ = value
        if min_ != 0 and info_value < min_:
            return False

        if max_ != 0 and info_value > max_:
            return False
        return True

    def _filter_list_str(self, key, value, info_value):
        for s in value:
            if self._filter_str(key, s, info_value):
                return True
        return False

    def _filter_str(self, key, value, info_value):
        return value.lower() == info_value.lower()

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
                if not self._filter_max_min(key, value, info_value):
                    return False
            else:
                if isinstance(value, list):
                    if not self._filter_list_str(key, value, info_value):
                        return False
                else:
                    if not self._filter_str(key, value, info_value):
                        return False

        return True

# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

from .common import MetaOnceObject
from .exceptions import NoConfig
from .helpers import import_object

"""
Set logger example:

    logger = logging.getLogger('console')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    _format = (
        '\x1b[37;40m[%(asctime)s] '
        '\x1b[36;40m%(levelname)8s '
        '%(name)s '
        '\x1b[32;40m%(filename)s'
        '\x1b[0m:'
        '\x1b[32;40m%(lineno)d '
        '\x1b[33;40m%(message)s\x1b[0m')
    formatter = logging.Formatter(
        _format,
        datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

Use him:

    options = Options()
    options.update(logger='console')
"""


class Options(object):

    DEFAULTS = {
        'workers': 4,
        'logger': 'console',
        'step_delay': 0.5,
        'delay_method': 'time.sleep',
        'backend': {
        },
        'periodic_backend': {
        },
        'backend_cls': 'simwg.RedisTaskBackend',
        'periodic_backend_cls': 'simwg.ConfigFilePeriodicTaskBackend',
    }

    __metaclass__ = MetaOnceObject

    _content = None

    def update(self, **options):
        if self._content is None:
            self._content = dict()
        self._content.update(options)

    def _get_by_field(self, field, rtype=None, default=0):
        if not isinstance(self._content, dict):
            raise NoConfig()
        values = (
            self._content.get(field) or
            self.DEFAULTS.get(field))
        result = default if values is None else values
        if rtype:
            result = rtype(result)
        return result

    @property
    def worker_count(self):
        return self._get_by_field('workers', int)

    @property
    def logger_name(self):
        return self._get_by_field('logger', str, '')

    @property
    def delay(self):
        return self._get_by_field('step_delay', float)

    @property
    def task_backend_options(self):
        return self._get_by_field('backend', dict, None)

    @property
    def task_backend_cls(self):
        path = self._get_by_field('backend_cls', str, '')
        return import_object(path)

    @property
    def periodic_task_backend_options(self):
        return self._get_by_field('periodic_backend', dict, None)

    @property
    def periodic_task_backend_cls(self):
        path = self._get_by_field('periodic_backend_cls', str, '')
        return import_object(path)

    @property
    def delay_method_name(self):
        return self._get_by_field('delay_method', str, '')

    @property
    def delay_method(self):
        return import_object(self.delay_method_name)

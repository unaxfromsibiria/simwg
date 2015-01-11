# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

import logging

from .common import BaseEnum
from .config import Options


class TaskResultStatus(BaseEnum):
    DONE = 1
    FAILED = 2


class TaskPriorityEnum(BaseEnum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    SUPREME = 3


class TaskResult(dict):

    def __init__(
            self,
            status=TaskResultStatus.DONE,
            error=None,
            content=None):

        assert status in TaskResultStatus.values()
        super(TaskResult, self).__init__()
        self.update(
            status=status,
            error=error,
            content=content)

    @property
    def status(self):
        return self.get('status')

    @property
    def error(self):
        return self.get('error')

    @property
    def content(self):
        return self.get('content')


class TaskData(object):

    # Simple default timeout near 1 hours are harmful, I think.
    # If you really need a timeout,
    # implement inside the procedure.
    # You can not just terminate any algorithm because it execute
    # 1 hour 1 minutes instead of 1 hour 0 minute
    DEFAULT_TIMEOUT = 3600 * 24

    _method = None
    _params = None
    _taken = None
    _timeout = None
    _tid = None
    _key = None
    _result = None
    _returned = None
    _priority = None
    _delay = 0

    def __init__(
            self,
            key,
            tid,
            method,
            taken=None,
            params=None,
            timeout=DEFAULT_TIMEOUT,
            result=None,
            returned=None,
            priority=TaskPriorityEnum.NORMAL,
            delay=0,
            **kwargs):

        if isinstance(result, dict):
            self._result = TaskResult(**result)

        self._key = key
        self._delay = delay
        self._tid = tid
        self._method = method
        self._taken = taken
        self._params = params
        self._returned = returned
        self._timeout = timeout
        self._priority = priority

    def __unicode__(self):
        return u"{}:{}".format(self._method, self._key)

    @property
    def id(self):
        return self._tid

    @property
    def key(self):
        return self._key

    @property
    def method(self):
        return self._method or ''

    @property
    def timeout(self):
        return self._timeout or self.DEFAULT_TIMEOUT

    @property
    def delay(self):
        return float(self._delay or 0)

    @property
    def params(self):
        return self._params

    @property
    def returned(self):
        return self._returned

    @returned.setter
    def returned(self, value):
        try:
            self._returned = float(value or 0)
        except (ValueError, TypeError) as err:
            options = Options()
            logger = logging.getLogger(options.logger_name)
            logger.warning(err)
            self._returned = None

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, value):
        if isinstance(value, dict):
            self._result = TaskResult(**value)

    def as_dict(self):
        result_dict = {
            'timeout': self._timeout or TaskData.DEFAULT_TIMEOUT,
            'tid': self._tid,
            'method': self._method,
            'delay': self._delay,
            'taken': self._taken,
            'returned': self._returned,
            'params': self._params,
            'priority': self._priority,
            'result': None,
        }

        if self._result:
            result_dict.update(result=dict(**self._result))
        return result_dict

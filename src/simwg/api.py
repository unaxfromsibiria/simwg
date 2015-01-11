# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

from .common import MetaOnceObject
from .config import Options
from .task import TaskData, TaskPriorityEnum
from .task_src import (
    BaseTaskBackend, ManageCommandEnum)


class TaskBackendAdapter(object):

    __metaclass__ = MetaOnceObject

    _task_src = None

    def _init(self):
        if self._task_src is None:
            options = Options()
            task_cls = options.task_backend_cls
            assert issubclass(task_cls, BaseTaskBackend)
            self._task_src = task_cls(options)

    def new_task_key(self):
        self._init()
        return self._task_src.new_task_key()

    def new_task(self, task):
        self._init()
        if isinstance(task, TaskData):
            self._task_src.update_task(task)

    def set_manage_command(self, command, params):
        return self._task_src.set_manage_command(command, params)


class SimwgTask(object):

    _method = None
    _timeout = None
    _priority = None

    def __init__(
            self,
            method,
            timeout=TaskData.DEFAULT_TIMEOUT,
            priority=TaskPriorityEnum.NORMAL):

        if callable(method):
            self._method = method
            self._timeout = timeout
            self._priority = priority
        else:
            raise TypeError('{} is callable?'.format(method))

    def _send(self, params=None, delay=0, priority=None):
        task_params = None
        if isinstance(params, dict):
            task_params = params

        task_src = TaskBackendAdapter()
        if isinstance(priority, int):
            task_priority = priority
        else:
            task_priority = self._priority

        key = task_src.new_task_key()
        task_src.new_task(TaskData(
            key=key,
            tid=key,
            delay=delay,
            priority=task_priority,
            method=u'{}.{}'.format(
                self._method.__module__,
                self._method.__name__),
            params=task_params,
            timeout=self._timeout))

    def run(
            self,
            params,
            async=True,
            delay=0,
            priority=None):
        # TODO: priority soon
        if self._method:
            if not isinstance(params, dict):
                raise TypeError('params is dict?')

            if async:
                self._send(params, delay=delay, priority=priority)
            else:
                self._method(**params)


def free_worker(worker_index):
    """
    Free worker of pool in manager (if any problem)
    :param int worker_index: index of worker [1..max_index]
    """

    task_src = TaskBackendAdapter()
    task_src.set_manage_command(
        ManageCommandEnum.FAKE_FREE_WORKER,
        {'worker': worker_index})

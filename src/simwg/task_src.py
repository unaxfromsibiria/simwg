# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

import time
import redis
import uuid
import cPickle as pickle
from .common import BaseEnum
from .config import Options
from .task import TaskData, TaskPriorityEnum


class ManageCommandEnum(BaseEnum):

    FAKE_FREE_WORKER = 'fakefree'


class BaseTaskBackend(object):
    _conf = None

    def __init__(self, options):
        assert isinstance(options, Options)
        self._conf = options.task_backend_options

    @property
    def _rand_line(self):
        return unicode(uuid.uuid4()).replace('-', '')

    def new_task_key(self, **params):
        raise NotImplementedError()

    def pop_task(self):
        raise NotImplementedError()

    def update_task(self, task):
        raise NotImplementedError()

    def info(self):
        raise NotImplementedError()

    def set_manage_command(self, command, params):
        raise NotImplementedError()

    def select_manage_command(self, command):
        raise NotImplementedError()


class RedisTaskBackend(BaseTaskBackend):

    _connection = None
    _key_prefix = 'simwg_'
    _default_task_timeout = TaskData.DEFAULT_TIMEOUT

    def __init__(self, options):
        super(RedisTaskBackend, self).__init__(options)
        pool = redis.ConnectionPool(**self._conf)
        self._connection = redis.Redis(connection_pool=pool)

    def new_task_key(self, **params):
        priority = params.get('priority')
        new_key = u'{}task_{}_{}'.format(
            self._key_prefix,
            priority or TaskPriorityEnum.NORMAL,
            self._rand_line)
        return new_key

    def info(self):
        # check connections
        info_dict = self._connection.info()
        info = u'\n'.join([
            '\t{}: {}'.format(*items)
            for items in info_dict.iteritems()])
        return info

    def pop_task(self):
        result = None
        task_keys = self._connection.keys(
            '{}task_*'.format(self._key_prefix))

        if task_keys:
            operation_time = time.time()
            for task_key in sorted(task_keys, reverse=True):
                # lock task
                try:
                    task_data = pickle.loads(
                        self._connection.get(task_key))
                except pickle.UnpicklingError:
                    continue
                else:
                    assert isinstance(task_data, dict)
                    task_time = task_data.get('taken')
                    if task_time:
                        continue
                    # try to get this task
                    task_data['taken'] = operation_time
                    task_timeout = int(
                        task_data.get('timeout') or
                        self._default_task_timeout)

                    self._connection.setex(
                        task_key,
                        time=task_timeout,
                        value=pickle.dumps(task_data))
                    # reread and check
                    task_data = pickle.loads(
                        self._connection.get(task_key))

                    if operation_time == task_data.get('taken'):
                        result = TaskData(
                            key=task_key,
                            **task_data)
                        break
                    else:
                        # ops! other server get him fist
                        # just try to get next
                        continue
        return result

    def update_task(self, task):
        assert isinstance(task, TaskData)
        task_data = task.as_dict()
        self._connection.setex(
            task.key,
            time=task.timeout,
            value=pickle.dumps(task_data))

    def set_manage_command(self, command, params):
        if not(params and isinstance(params, dict)):
            raise TypeError('params incorrect')

        if command and isinstance(command, basestring):
            new_key = u'{}command_{}_{}'.format(
                self._key_prefix, command, self._rand_line)
            self._connection.setex(
                new_key,
                time=self._default_task_timeout,
                value=pickle.dumps(params))
        else:
            raise TypeError('command incorrect')

    def select_manage_command(self, command):
        result = {}
        if command and isinstance(command, basestring):
            keys = u'{}command_{}_*'.format(self._key_prefix, command)
            commands = self._connection.keys(keys)
            if commands:
                geted = []
                for command_key in commands:
                    try:
                        result[command_key] = pickle.loads(
                            self._connection.get(command_key))
                    except pickle.UnpicklingError:
                        continue
                    else:
                        geted.append(command_key)

                self._connection.delete(*geted)

        else:
            raise TypeError('command incorrect')
        return result

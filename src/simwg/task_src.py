# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

import logging
import redis
import time
import uuid
import ConfigParser
import cPickle as pickle
from abc import ABCMeta
from datetime import datetime, time as time_cls, timedelta
from .common import BaseEnum
from .config import Options
from .task import TaskData, TaskPriorityEnum, TaskTypeEnum
from .helpers import import_object


class ManageCommandEnum(BaseEnum):

    FAKE_FREE_WORKER = 'fakefree'


class TaskBackend(object):
    _conf = None
    _logger = None
    _option_attr = None
    __metaclass__ = ABCMeta

    def info(self):
        raise NotImplementedError()

    def __init__(self, options):
        assert isinstance(options, Options)
        assert isinstance(self._option_attr, basestring)
        self._conf = getattr(options, self._option_attr)
        self._logger = logging.getLogger(options.logger_name)


class BasePeriodicTaskBackend(TaskBackend):

    _option_attr = 'periodic_task_backend_options'
    _main_backend = None

    def set_main_backend(self, main_backend):
        self._main_backend = main_backend

    def get_tasks(self, at=None, one=False):
        raise NotImplementedError()

    def pop_task(self):
        raise NotImplementedError()

    def create_task(self, method):
        priority = int(
            self._conf.get('priority') or TaskPriorityEnum.NORMAL)
        key = self._main_backend.new_task_key(priority=priority)

        return TaskData(
            key=key,
            tid=key,
            task_type=TaskTypeEnum.PERIODIC,
            method=method,
            priority=priority)

    def now(self):
        """
        this date time
        """
        return datetime.now()


class BaseTaskBackend(TaskBackend):
    _option_attr = 'task_backend_options'

    @property
    def _rand_line(self):
        return unicode(uuid.uuid4()).replace('-', '')

    def new_task_key(self, **params):
        raise NotImplementedError()

    def pop_task(self):
        raise NotImplementedError()

    def update_task(self, task):
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


class ConfigFilePeriodicTaskBackend(BasePeriodicTaskBackend):
    """
    Configuration file has format:
    [everytime]
    <metod.full.path>=<start time 00:00> <every N minutes>

    example:
    web_project.tools.send_mail=23:59 10
    forever run every 10 minutes
    web_project.tools.build_cache=06:00 0
    once run this procedure at 06:00 every day
    web_project.tools.promo_update=20:00 50
    run procedure every 50 minute after 20:00
    """
    _run_at = {}

    def info(self):
        try:
            file_content = open(self._conf['path']).read()
        except KeyError:
            file_content = 'Error: no path in options!'
        except Exception as err:
            file_content = 'Error: {}'.format(err)
        return file_content

    def get_tasks(self, at=None, one=False):
        assert isinstance(self._main_backend, BaseTaskBackend)
        section = 'everytime'
        config = ConfigParser.ConfigParser()
        try:
            config.read(self._conf['path'])
        except Exception as err:
            # fake
            config = {section: []}
            self._logger.error(
                'No backend file for periodic tasks: {}'.format(
                    err))

        if not isinstance(at, datetime):
            at = self.now()

        tasks = {}
        result = None
        try:
            methods = config.options(section)
        except Exception as err:
            self._logger.warning(err)
            methods = []

        for method in methods:
            line = config.get(section, method)
            run_at = str(line).split()
            if len(run_at) == 2:
                time_str, period = run_at
            else:
                self._logger.warning(
                    'wrong line {}={} in conf: {}'.format(
                        method, line, self._conf['path']))
                continue

            try:
                period = int(period)
            except (TypeError, ValueError):
                self._logger.warning(
                    'wrong line {}={} in conf: {} period not int'.format(
                        method, line, self._conf['path']))
                continue

            if not(0 <= period <= 60):
                self._logger.warning(
                    'wrong line {}={} in conf: {} period incorrect'.format(
                        method, line, self._conf['path']))
                continue

            try:
                start_run = datetime.combine(
                    at.date(),
                    time_cls(*map(int, time_str.split(':'))))
            except (TypeError, ValueError):
                self._logger.warning(
                    'wrong line {}={} in conf: {} time incorrect'.format(
                        method, line, self._conf['path']))
                continue

            last_run = self._run_at.get(method)

            if period == 0:
                period = 24 * 60

            if not last_run:
                self._run_at[method] = (
                    start_run - timedelta(seconds=60 * period + 1))
                continue

            need_run = bool(
                at >= start_run and
                at >= last_run + timedelta(seconds=60 * period))

            if need_run and method not in tasks:
                try:
                    method_func = import_object(method)
                except Exception as err:
                    method_func = None

                if callable(method_func):
                    new_task = self.create_task(method)
                    self._run_at[method] = at
                    if one:
                        # only one task need
                        result = new_task
                        break
                    else:
                        tasks[method] = new_task
                else:
                    self._logger.warning(
                        'wrong line {}={} in conf: {} bad method'.format(
                            method, line, self._conf['path']))

        if not one:
            result = tasks.values()

        return result

    def pop_task(self):
        return self.get_tasks(one=True)

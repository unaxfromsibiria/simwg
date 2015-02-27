# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

import logging
import inspect
import gc
import signal
import time

from Queue import Empty as QueueEmpty
from multiprocessing import Process, Queue
from random import SystemRandom

from . import msg as this_msg
from .config import Options
from .helpers import import_object
from .task import TaskResultStatus, TaskTypeEnum
from .task_src import (
    BaseTaskBackend, ManageCommandEnum,
    BasePeriodicTaskBackend)
from .worker import process_worker


class WorkerPool(object):
    _workers = None
    _random = None

    def __init__(self, size):
        self._random = SystemRandom()
        n = int(size or 0)
        if n:
            self._workers = []
            for i in range(size or 0):
                self._workers.append({
                    'busy': False,
                    'name': i + 1,
                })

    def get_free(self, random=True):
        free_index = [
            index
            for index, worker_data in enumerate(self._workers)
            if not worker_data.get('busy')
        ]

        if free_index:
            if random:
                result = self._random.choice(free_index)
            else:
                result = free_index[0]
        else:
            result = None

        return result

    def count_free(self):
        result = 0
        for worker_data in self._workers:
            if not worker_data.get('busy'):
                result += 1
        return result

    def is_all_free(self):
        statuses = [
            not worker_data.get('busy')
            for worker_data in self._workers]
        return len(statuses) < 1 or all(statuses)

    def free(self, index):
        self._workers[index].update(busy=False)

    def busy(self, index):
        self._workers[index].update(busy=True)


class WorkerManager(object):

    _logger = None
    _workers = None
    _is_work = False
    _delay_method = None
    _delay_method_name = None
    _delay = 0
    _task_src = None
    _periodic_task_src = None
    _result_tasks_queue = None
    _worker_task_data = None
    _main_frame_id = None
    _methods_cache = None

    def __init__(self, options):
        """
        :param Options options: configuration object
        """

        assert isinstance(options, Options)
        self._logger = logging.getLogger(options.logger_name)
        self._workers = WorkerPool(options.worker_count)
        self._delay = options.delay
        self._delay_method = options.delay_method
        self._delay_method_name = options.delay_method_name
        assert callable(self._delay_method)
        task_cls = options.task_backend_cls
        assert issubclass(task_cls, BaseTaskBackend)
        self._task_src = task_cls(options)
        periodic_task_cls = options.periodic_task_backend_cls
        assert issubclass(periodic_task_cls, BasePeriodicTaskBackend)
        self._periodic_task_src = periodic_task_cls(options)
        self._periodic_task_src.set_main_backend(self._task_src)
        self._methods_cache = {}

        self._worker_task_data = {}
        info_msg_tpl = (
            u'{}:{} created\n'
            u'Backend info:\n{}\n'
            u'Periodic backend info: {}')
        self._logger.info(
            info_msg_tpl.format(
                self.__class__.__name__,
                id(self),
                self._task_src.info(),
                self._periodic_task_src.info()))

        def signal_term_handler(signal, frame):
            if self._is_work and self._main_frame_id == id(frame):
                # in main frame
                self._is_work = False
                self._logger.info(this_msg.stopping)

        signal.signal(signal.SIGINT, signal_term_handler)
        signal.signal(signal.SIGTERM, signal_term_handler)

    def start(self):
        if not self._is_work:
            self._is_work = True
            self._result_tasks_queue = Queue()
            self._run()

    def _run(self):
        self._main_frame_id = id(inspect.currentframe())
        is_work = self._is_work
        delay_method = self._delay_method
        last_free_indexes = set()

        while is_work:
            is_work = not(
                not self._is_work and self._workers.is_all_free())

            if is_work:
                # free any one
                last_free_indexes.clear()

                free_task_data = True

                while free_task_data:
                    try:
                        free_task_data = self._result_tasks_queue.get(
                            block=False)
                    except QueueEmpty:
                        free_task_data = False

                    if free_task_data:
                        task_key, worker_index, result = free_task_data

                        task = self._worker_task_data[worker_index]
                        # set result
                        if task.key == task_key:
                            last_free_indexes.add(worker_index)
                            task.result = result
                            task.returned = time.time()
                            if task.type == TaskTypeEnum.RPC:
                                self._task_src.update_task(task)
                        else:
                            self._logger.error(
                                u'{} != {} wtf!?'.format(task.key, task_key))

                        self._workers.free(worker_index)
                        self._logger.info(
                            this_msg.task_free.format(
                                task_key, worker_index + 1))
                        del task

                # fake free
                free_workers_data = self._task_src.select_manage_command(
                    ManageCommandEnum.FAKE_FREE_WORKER)

                if free_workers_data:

                    for free_data in free_workers_data.values():
                        # real index = user index - 1
                        need_free_index = free_data.get('worker') - 1

                        if need_free_index not in last_free_indexes:
                            task = self._worker_task_data.get(need_free_index)

                            if task:
                                task.result = dict(
                                    error=this_msg.fake_worker_free,
                                    status=TaskResultStatus.FAILED)
                                task.returned = time.time()
                                self._task_src.update_task(task)
                                self._logger.warning(
                                    this_msg.fake_end_task.format(
                                        task.key, need_free_index))

                self._step()
                delay_method(self._delay)
                gc.collect()

    def stop(self):
        self._is_work = False

    def _step(self):
        count_free_workers = self._workers.count_free()
        logger = self._logger

        if count_free_workers and not self._is_work:
            # wait stopping
            logger.warning(this_msg.ignor_new_tasks)
        elif count_free_workers:

            for _ in xrange(count_free_workers):
                # periodic task first
                task = self._periodic_task_src.pop_task()
                # onece tasks
                if not task:
                    task = self._task_src.pop_task()

                if task:
                    free_worker_index = self._workers.get_free()
                    logger.debug(
                        this_msg.selected_task.format(task))

                    if task.method not in self._methods_cache:
                        try:
                            # check
                            task_method = import_object(task.method)
                        except Exception as err:
                            logger.debug(err)
                        else:
                            if callable(task_method):
                                self._methods_cache[task.method] = task_method

                    if task.method in self._methods_cache:
                        # all ok, create process
                        worker = Process(
                            target=process_worker,
                            name='worker_{}'.format(free_worker_index + 1),
                            kwargs={
                                'index': free_worker_index,
                                'task': task.key,
                                'task_type': task.type,
                                'result_queue': self._result_tasks_queue,
                                'method': task.method,
                                'params': task.params,
                                'delay': task.delay,
                                'delay_method': self._delay_method_name,
                                'logger_name': logger.name,
                                'timeout': task.timeout,
                            })
                        # wait result here
                        self._worker_task_data[free_worker_index] = task
                        self._workers.busy(free_worker_index)
                        logger.info(
                            this_msg.started_task.format(
                                task,
                                TaskTypeEnum.key(task.type),
                                free_worker_index + 1))
                        worker.start()
                    else:
                        logger.error(
                            this_msg.no_methods.format(task, task.method))
                else:
                    break

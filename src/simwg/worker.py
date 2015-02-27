# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

import logging

from . import msg as this_msg
from .helpers import import_object
from .task import TaskResultStatus, TaskTypeEnum


def process_worker(
        index,
        result_queue,
        task,
        task_type,
        method,
        params,
        delay,
        delay_method,
        logger_name,
        timeout):
    """
    :param int index: worker index
    :param str task: task key
    :param Queue result_queue: result aggregator
    :param str method: method full path
    :param str delay_method: time delay method full path
    :param dict params: method kwargs
    :param float params: delay before call method
    :param str logger_name: used logger name
        (logger used as argument for target method)
    :param float timeout: for target method argument
    """

    result = dict(
        error=None,
        content=None,
        status=TaskResultStatus.DONE)

    if not isinstance(params, dict):
        params = {}

    logger = logging.getLogger(logger_name)
    if delay:
        logger.info(
            this_msg.task_delay_wait.format(task, index + 1, delay))
        do_delay = import_object(delay_method)
        do_delay(delay)

    error_msg = this_msg.task_run_error_tpl.format(task, index + 1)
    try:
        target_method = import_object(method)
    except Exception as err:
        err_msg = u'{}: {}'.format(err.__class__.__name__, err)
        logger.error(error_msg.format(err_msg))
        result.update(
            error=err_msg,
            status=TaskResultStatus.FAILED)
    else:
        if 'logger' not in params:
            params.update(logger=logger)

        # runtime limit for you procedure
        # must implemented inside
        if 'timeout' not in params:
            params.update(timeout=timeout)

        try:
            content = target_method(**params)
            if isinstance(content, basestring):
                result.update(content=content)
        except Exception as err:
            err_msg = u'{}: {}'.format(err.__class__.__name__, err)
            logger.error(error_msg.format(err_msg))
            result.update(
                error=err_msg,
                status=TaskResultStatus.FAILED)

    result_queue.put((task, index, result))

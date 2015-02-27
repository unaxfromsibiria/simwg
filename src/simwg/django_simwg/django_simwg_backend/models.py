# -*- coding: utf-8 -*-
'''
Created on 25 февр. 2015 г.

@author: Michael Vorotyntsev

'''

from django.db import models

from simwg.api import TaskPriorityEnum
from simwg.common import BaseEnum
from simwg.task import TaskData, TaskTypeEnum
try:
    import cPickle as pickler
except ImportError:
    import pickle as pickler


class TaskStatusEnum(BaseEnum):

    NEW = 0
    WAIT = 1
    PROCESSING = 2
    DONE = 3
    FALID = 4


class BaseTaskModel(models.Model):

    accept_types = []

    method = models.TextField(default='')
    # not use JsonField
    params_content = models.TextField(default='')
    result_content = models.TextField(default='')
    timeout = models.IntegerField(default=0)
    delay = models.IntegerField(default=0)

    created = models.DateTimeField(auto_now_add=True)

    priority = models.IntegerField(
        default=TaskPriorityEnum.NORMAL,
        choices=TaskPriorityEnum.as_choices())

    status = models.IntegerField(
        default=TaskStatusEnum.NEW,
        choices=TaskStatusEnum.as_choices())

    class Meta:
        abstract = True

    @property
    def result(self):
        if self.result_content:
            result = pickler.loads(self.result_content)
        else:
            result = None
        return result

    @result.setter
    def result(self, value):
        if value:
            self.result_content = pickler.dumps(value)
        else:
            self.result_content = ''

    @property
    def params(self):
        if self.params_content:
            result = pickler.loads(self.params_content)
        else:
            result = None
        return result

    @params.setter
    def params(self, value):
        if value:
            self.params_content = pickler.dumps(value)
        else:
            self.params_content = ''

    def update_data(self, task):
        assert isinstance(task, TaskData)
        assert task.type in self.accept_types
        self.params = task.params
        self.result = task.result
        self.timeout = task.timeout
        self.priority = task.priority
        self.delay = task.delay

    def get_advanced_task_data(self):
        """
        for reload
        """
        return {}

    def create_task_data(self):
        assert bool(self.pk)

        task = TaskData(
            key=self.pk,
            tid=self.pk,
            method=self.method,
            params=self.params,
            priority=self.priority,
            delay=self.delay,
            timeout=self.timeout,
            **self.get_advanced_task_data())
        return task


class TaskModel(BaseTaskModel):
    """
    Main task storage orm model
    """
    accept_types = [TaskTypeEnum.RPC]

    taken = models.DateTimeField(null=True, default=None)
    returned = models.DateTimeField(null=True, default=None)

    class Meta:
        db_table = 'simwg_task'
        ordering = ['priority', 'created']

    def update_data(self, task):
        super(TaskModel, self).update_data(task)
        self.taken = task.taken
        self.returned = task.returned

    def get_advanced_task_data(self):
        result = super(TaskModel, self).get_advanced_task_data()
        result.update(
            taken=self.taken,
            returned=self.returned,
            task_type=TaskTypeEnum.RPC)
        return result


class PeriodicTaskModel(BaseTaskModel):
    accept_types = [TaskTypeEnum.PERIODIC]

    run_last = models.DateTimeField(null=True, default=None)
    run_next = models.DateTimeField(null=True, default=None)
    run_count = models.IntegerField(default=0)
    period = models.IntegerField(default=0)
    start_time = models.TimeField()

    class Meta:
        db_table = 'simwg_periodic_task'
        ordering = ['priority', 'created']

    @property
    def once(self):
        return self.period == 0

    def get_advanced_task_data(self):
        result = super(TaskModel, self).get_advanced_task_data()
        result.update(
            taken=self.run_last,
            task_type=TaskTypeEnum.PERIODIC)
        return result


class ManageCommandModel(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    name = models.TextField(default='')
    params_content = models.TextField(default='')
    taken = models.DateTimeField(null=True, default=None)

    class Meta:
        db_table = 'simwg_manage_command'

    @property
    def params(self):
        if self.params_content:
            result = pickler.loads(self.params_content)
        else:
            result = None
        return result

    @params.setter
    def params(self, value):
        if value:
            self.params_content = pickler.dumps(value)
        else:
            self.params_content = ''

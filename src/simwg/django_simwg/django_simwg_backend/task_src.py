# -*- coding: utf-8 -*-
'''
Created on 26 февр. 2015 г.

@author: Michael Vorotyntsev

'''

from datetime import datetime, timedelta

from django.db.models import Q
from django.utils.timezone import now as datetime_now

from simwg.task import TaskData
from simwg.task_src import (
    BaseTaskBackend, BasePeriodicTaskBackend)

from .models import (
    TaskModel, TaskStatusEnum, ManageCommandModel,
    PeriodicTaskModel)


class OrmDjangoTaskBackend(BaseTaskBackend):

    def __init__(self, options):
        super(OrmDjangoTaskBackend, self).__init__(options)

    def new_task_key(self, **params):
        record = TaskModel()
        record.save()
        return record.pk

    def info(self):
        return 'Django ORM model {}'.format(TaskModel)

    def pop_task(self):
        result = None
        tasks = TaskModel.objects.filter(
            taken__isnull=True,
            status=TaskStatusEnum.WAIT).order_by(
                '-priority', '-created').select_for_update()
        try:
            record = tasks[:1].get()
        except TaskModel.DoesNotExist:
            result = None
        else:
            record.status = TaskStatusEnum.PROCESSING
            record.taken = datetime_now()
            record.save()
            result = record.create_task_data()

        return result

    def update_task(self, task):
        assert isinstance(task, TaskData)
        try:
            record = TaskModel.objects.get(pk=task.key)
        except TaskModel.DoesNotExist:
            self._logger.error(
                'Does not exists record! id:{}'.format(task.key))
        else:
            record.update_data(task)

    def set_manage_command(self, command, params):
        if not(params and isinstance(params, dict)):
            raise TypeError('params incorrect')

        if command and isinstance(command, basestring):
            command = ManageCommandModel(name=command)
            command.params = params
            command.save()
        else:
            raise TypeError('command incorrect')

    def select_manage_command(self, command):
        result = {}
        if command and isinstance(command, basestring):
            commands = ManageCommandModel.objects.filter(
                taken__isnull=True,
                name=command).order_by(
                    '-created').select_for_update()

            for record in commands:
                result[record.id] = record.params
            commands.update(taken=datetime_now())

        else:
            raise TypeError('command incorrect')
        return result


class OrmDjangoPeriodicTaskBackend(BasePeriodicTaskBackend):
    """
    Django ORM backend for periodic tasks
    """

    def info(self):
        return 'Django ORM model {}'.format(PeriodicTaskModel)

    def now(self):
        return datetime_now()

    def calc_next_run(self):
        records = PeriodicTaskModel.objects.filter(
            run_next__isnull=True).select_related()

        now = self.now()
        now_date = now.date()
        for record in records:
            record.run_last = None
            record.run_next = datetime.combine(
                now_date, record.start_time)
            record.save()

    def get_tasks(self, at=None, one=False):
        assert isinstance(self._main_backend, BaseTaskBackend)
        self.calc_next_run()

        run_at = self.now()

        records = PeriodicTaskModel.objects.filter(
            Q(period__gt=0) | (Q(period=0) & Q(run_last__isnull=True)),
            run_next__gte=run_at).order_by(
                'run_next').select_related()

        result = []
        for record in records:
            record.run_last = run_at
            run_count = int(record.run_count or 0) + 1

            if not record.once:
                record.run_next = (
                    run_at + timedelta(seconsd=60 * record.period))
            record.run_count = run_count
            record.save()
            result.append(record.create_task_data())

            if one:
                break

        if one:
            try:
                result = result[0]
            except IndexError:
                result = None

        return result

    def pop_task(self):
        return self.get_tasks(one=True)

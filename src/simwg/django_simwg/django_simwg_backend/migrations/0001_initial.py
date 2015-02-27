# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='ManageCommandModel',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('name', models.TextField(default=b'')),
                ('params_content', models.TextField(default=b'')),
                ('taken', models.DateTimeField(default=None, null=True)),
            ],
            options={
                'db_table': 'simwg_manage_command',
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='PeriodicTaskModel',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('method', models.TextField(default=b'')),
                ('params_content', models.TextField(default=b'')),
                ('result_content', models.TextField(default=b'')),
                ('timeout', models.IntegerField(default=0)),
                ('delay', models.IntegerField(default=0)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('priority', models.IntegerField(default=3, choices=[(b'HIGH', 4), (b'SUPREME', 5), (b'LOW', 2), (b'NORMAL', 3)])),
                ('status', models.IntegerField(default=0, choices=[(b'NEW', 0), (b'PROCESSING', 2), (b'FALID', 4), (b'DONE', 3), (b'WAIT', 1)])),
                ('run_last', models.DateTimeField(default=None, null=True)),
                ('run_next', models.DateTimeField(default=None, null=True)),
                ('run_count', models.IntegerField(default=0)),
                ('period', models.IntegerField(default=0)),
                ('start_time', models.TimeField()),
            ],
            options={
                'ordering': ['priority', 'created'],
                'db_table': 'simwg_periodic_task',
            },
            bases=(models.Model,),
        ),
        migrations.CreateModel(
            name='TaskModel',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('method', models.TextField(default=b'')),
                ('params_content', models.TextField(default=b'')),
                ('result_content', models.TextField(default=b'')),
                ('timeout', models.IntegerField(default=0)),
                ('delay', models.IntegerField(default=0)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('priority', models.IntegerField(default=3, choices=[(b'HIGH', 4), (b'SUPREME', 5), (b'LOW', 2), (b'NORMAL', 3)])),
                ('status', models.IntegerField(default=0, choices=[(b'NEW', 0), (b'PROCESSING', 2), (b'FALID', 4), (b'DONE', 3), (b'WAIT', 1)])),
                ('taken', models.DateTimeField(default=None, null=True)),
                ('returned', models.DateTimeField(default=None, null=True)),
            ],
            options={
                'ordering': ['priority', 'created'],
                'db_table': 'simwg_task',
            },
            bases=(models.Model,),
        ),
    ]

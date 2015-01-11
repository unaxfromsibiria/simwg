# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

from django.conf import settings
from django.core.management.base import (
    BaseCommand, CommandError)
from optparse import make_option


SETTINGS_FIELD = 'SIMWG_SETTINGS'


class Command(BaseCommand):

    args = '<simwg_service --start>'
    help = u'Simwg service run'

    option_list = BaseCommand.option_list + (
        make_option(
            '--start',
            action='store_true',
            dest='start',
            default=False,
            help=u'Start service.'),
    )

    def handle(self, *args, **options):
        start = options.get('start')

        if not start:
            raise CommandError(u'No one options!')

        conf = getattr(settings, SETTINGS_FIELD, None)
        if not isinstance(conf, dict):
            raise CommandError(
                u'No section {} in settings!'.format(SETTINGS_FIELD))

        options = conf.get('options')
        methods = conf.get('methods')

        if not(isinstance(methods, list) and methods):
            raise CommandError(u'No one methods in settings!')

        if not isinstance(options, dict):
            raise CommandError(u'Section "options" not found!')

        from simwg import WorkerManager, Options
        options_simwg = Options()
        options_simwg.update(**options)

        manager = WorkerManager(
            options=options_simwg,
            methods=methods)

        manager.start()

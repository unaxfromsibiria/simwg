# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''


class NoConfig(Exception):

    def __init__(self, msg='Set configuration!'):
        super(NoConfig, self).__init__(msg)

# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''

import importlib


def import_object(line):
    assert isinstance(line, basestring)
    parts = line.split('.')
    if len(parts) == 1:
        _locals = locals()
        result = _locals.get(parts[0])
    else:
        module = importlib.import_module(u'.'.join(parts[:-1]))
        result = getattr(module, parts[-1], None)
    return result

# -*- coding: utf-8 -*-
'''

@author: Michael Vorotyntsev

'''


class MetaOnceObject(type):
    """
    Once system object.
    """

    _classes = dict()

    def __call__(self, *args, **kwargs):
        cls = str(self)
        if cls not in self._classes:
            this = super(MetaOnceObject, self).__call__(*args, **kwargs)
            self._classes[cls] = this
        else:
            this = self._classes[cls]
        return this


class BaseEnum(object):
    @classmethod
    def as_dict(cls):
        result = {}
        for field in cls.__dict__:
            if field == field.upper():
                result[field] = getattr(cls, field)
        return result

    @classmethod
    def keys(cls):
        result = []
        for field in cls.__dict__:
            if field == field.upper():
                result.append(field)
        return result

    @classmethod
    def values(cls):
        result = []
        for field in cls.__dict__:
            if field == field.upper():
                result.append(getattr(cls, field))
        return result

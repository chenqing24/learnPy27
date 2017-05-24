#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

__author__ = 'Jeff Chen'

'''
封装orm操作
'''

import db

class Field(object):
    '''
    db的字段名和类型
    '''
    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type

    def __str__(self): # 定制类，反馈类实例的内部信息
        return '<%s:%s>' % (self.__class__.__name__, self.name)


class StringField(Field):
    '''
    扩展的str类型字段
    '''
    def __init__(self, name):
        super(StringField, self).__init__(name, 'varchar(100)')


class IntegerField(Field):
    '''
    扩展的int型字段
    '''
    def __init__(self, name):
        super(IntegerField, self).__init__(name, 'int')


class ModelMetaclass(type):
    '''
    元类，定义类的继承
    '''
    def __new__(cls, name, bases, attrs):
        if name == 'Model': # 基类直接返回
            return type.__new__(cls, name, bases, attrs)

        mappings = dict()
        for k, v in attrs.iteritems(): # 按类型装配结果集
            if isinstance(v, Field):
                mappings[k] = v

        for k in mappings.iterkeys():
            attrs.pop(k)

        attrs['__table__'] = name # 类名与表名一致
        attrs['__mappings__'] = mappings # 属性与列映射

        return type.__new__(cls, name, bases, attrs)


class Model(dict):
    '''
    orm基类
    '''
    __metaclass__ = ModelMetaclass

    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError('Model not find attr: %s' % item)

    def __setattr__(self, key, value):
        self[key] = value

    def save(self):
        fields = []
        params = []
        args = []
        for k, v in self.__mappings__.iteritems():
            fields.append(v.name)
            params.append('?')
            args.append(getattr(self, k, None))
        sql = 'insert into %s (%s) values (%s)' % (self.__table__, ','.join(fields), ','.join(params))
        logging.info('SQL: %s, ARGS: %s' % (sql, args))
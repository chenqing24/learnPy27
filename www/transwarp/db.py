#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import threading
import functools

__author__ = 'Jeff Chen'

'''
封装基本的SELECT、INSERT、UPDATE和DELETE操作的db模块
'''

class Dict(dict):
    '''
    反馈结果的字典
    '''
    def __init__(self, names=(), values=(), **kwargs):
        super(Dict, self).__init__(**kwargs)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, item):
        try:
            return self[item]
        except:
            logging.warning('Dict get error...')
            raise

    def __setattr__(self, key, value):
        self[key] = value


# db引擎对象
class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


# 全局变量engine：连接db的工厂
engine = None


# 懒连接对象
class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    # 返回当前connection
    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection

    # 提交
    def commit(self):
        self.connection.commit()


class _DbCtx(threading.local):
    '''
    db连接的context对象
    '''
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    # 主动初始化
    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    # 安全释放
    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    # 反馈当前游标
    def cursor(self):
        return self.connection.cursor()


# 全局变量_db_ctx：threading.local对象，保存线程本地数据，保证对象唯一性
_db_ctx = _DbCtx()


class _ConnectionCtx(object):
    '''
    数据库连接的上下文，目的是自动获取和释放连接
    '''

    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


def connection():
    '''
    反馈连接对象_ConnectionCtx()，可使用作用域，类似：
    with connection():
        pass
    '''
    return _ConnectionCtx()


def with_connection(func):
    '''
    装饰器：使用db连接
    @with_connection
    def do_some_db_operation():
        pass
    
    :param func: 
    :return: 
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        with _ConnectionCtx():
            return func(*args, **kwargs)
    return _wrapper


class _TransactionCtx(object):
    '''
    事务上下文，支持嵌套
    '''
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions += 1
        logging.info('begin trans...' if _db_ctx.transactions == 1 else 'join current trans...')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        _db_ctx.transactions -= 1
        try:
            if _db_ctx.transactions == 0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except:
            logging.warning('commit fail, try rollback...')
            _db_ctx.connection.rollback()
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback trans...')
        _db_ctx.connection.rollback()


def transaction():
    '''
    反馈db事务对象_TransactionCtx
    :return: 
    '''
    return _TransactionCtx()


def with_transaction(func):
    '''
    装饰器：使用db事务
    :param func: 
    :return: 
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        with _TransactionCtx():
            return func(*args, **kwargs)
    return _wrapper

def _select(sql, *args):
    '''
    执行sql，反馈结果
    :param sql: 
    :param args: 
    :return: 
    '''
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, args: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


@with_connection
def select(sql, *args):
    '''
    执行query，反馈结果
    :param sql: 
    :param args: 
    :return: 
    '''
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


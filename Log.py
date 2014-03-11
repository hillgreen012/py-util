#!/usr/bin/env python

import re
import sys
import os
import MySQLdb as mdb
import datetime
import random
import socket
import pdb

# mysql> desc sdhlog;
# +---------------------+---------------+------+-----+---------+-------+
# | Field               | Type          | Null | Key | Default | Extra |
# +---------------------+---------------+------+-----+---------+-------+
# | ID                  | decimal(22,0) | NO   | PRI | 0       |       | 
# | operator            | varchar(200)  | YES  |     | NULL    |       | 
# | operationType       | varchar(200)  | YES  |     | NULL    |       | 
# | operationTargetType | varchar(200)  | YES  |     | NULL    |       | 
# | operationTarget     | varchar(200)  | YES  |     | NULL    |       | 
# | description         | varchar(256)  | YES  |     | NULL    |       | 
# | operationTime       | datetime      | YES  |     | NULL    |       | 
# +---------------------+---------------+------+-----+---------+-------+

class Log:
    FATAL = 50
    ERROR = 40
    WARN = 30
    INFO = 20
    DEBUG = 10
    NOTSET = 0
    MAX_CACHE_SIZE = 10
    INSERT_STMT = """
    INSERT INTO gv_local.sdhlog (
    id,         operationTarget,    description,
    operationTargetType,
    operationTime,     
    operationType,     
    operator           
    )
    VALUES (
    %s,         %s,                 %s,
    %s,
    %s,
    %s, 
    %s
    )
    """
    CONFIG_NAMES = {
        FATAL:  'fatal',
        ERROR:  'error',
        WARN:   'warn',
        INFO:   'info',
        DEBUG:  'debug',
        }

    def __init__(self, **kwargs):
        self.__conn = None
        self.__cache = []
        self.__conn = mdb.connect(**kwargs)
        assert self.__conn
        # self.__ip = None
        # self.__ip = socket.gethostbyname(socket.gethostname())
        # assert self.__ip

    def __genId(self, now):
        id = now.strftime('%s')[2:] + str(now.microsecond).zfill(6)[0:3] + str(random.randint(0, 9999)).zfill(4)
        # id = now.strftime('%Y%m%dT%H%M%S') + str(now.microsecond).zfill(6) + '/' + \
        #      str(random.randint(0, 999999)).zfill(6)
        return id

    def __makeRecord(self, level, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc):
        now = datetime.datetime.now()
        id = self.__genId(now)
        opTime = now.strftime('%Y-%m-%d %H:%M:%S')
        randDesc = self.__class__.CONFIG_NAMES[level]
        # self.logger.warn('strategy', sql, 'FAILED', 'unknown', 'admin')
        # _log.info(_platform, _quest, level, 'SENSITIVE', _username)
        return (id, objDesc, randDesc + '#' + opResDesc + '#' + opDetails, 
                'sensitive_object_access',
                opTime,
                opTypeDesc, 
                subjectDesc)
        
    def warn(self, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc):
        return self.__log(Log.WARN, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc)

    def fatal(self, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc):
        return self.__log(Log.FATAL, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc)

    def info(self, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc):
        return self.__log(Log.INFO, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc)

    def debug(self, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc):
        return self.__log(Log.DEBUG, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc)
        
    def error(self, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc):
        return self.__log(Log.ERROR, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc)
        
    def __log(self, level, objDesc, opDetails, opResDesc, opTypeDesc, subjectDesc):
        # record = self.__makeRecord(level, objDesc, opDetails, opResDesc,
        #                            opTypeDesc, subjectDesc, self.__ip)
        record = self.__makeRecord(level, objDesc, opDetails, opResDesc,
                                   opTypeDesc, subjectDesc)
        self.__cache.append(record)
        return self.flush()

    def flush(self):
        if not self.__cache or len(self.__cache) < self.__class__.MAX_CACHE_SIZE:
            return False
        return flushIfAny()

    def flushIfAny(self):
        cur = None
        try:
            cur = self.__conn.cursor()
            cur.executemany(self.__class__.INSERT_STMT, self.__cache)
            self.__conn.commit()
            self.__cache = []
        finally:
            if cur:
                cur.close()
                cur = None
        return True
        
    def __del__(self):
        if self.__conn:
            if self.__cache:
                self.flushIfAny()
            self.__conn.close()
            self.__conn = None

def __Log_ut():
    # logger = Log(host='localhost', port=3306, unix_socket='/tmp/my_mysql.sock', \
    logger = Log(host='127.0.0.1', port=3309, \
                 user='root', passwd='root123', \
                 db='gv_local', use_unicode=True, charset='utf8')
    logger.warn('strategy', 'sql', 'FAILED', 'unknown', 'admin')
    logger.info('platform', '_quest', 'level', 'SENSITIVE', '_username')
    logger.warn('objDesc', 'unit_test', 'opResDesc', 'opTypeDesc', 'subjectDesc')
    logger.fatal('objDesc', 'unit_test', 'opResDesc', 'opTypeDesc', 'subjectDesc')
    logger.info('objDesc', 'unit_test', 'opResDesc', 'opTypeDesc', 'subjectDesc')
    logger.debug('objDesc', 'unit_test', 'opResDesc', 'opTypeDesc', 'subjectDesc')
    logger.error('objDesc', 'unit_test', 'opResDesc', 'opTypeDesc', 'subjectDesc')
    logger.flushIfAny()
    print 'Log Success!'

if __name__ == '__main__':
    __Log_ut()

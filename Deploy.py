#!/usr/bin/env python

import MySQLdb as mdb

_conn = None
_cur = None

def Connect(**kwargs):
    global _conn
    global _cur
    _conn = mdb.connect(**kwargs)
    _cur = _conn.cursor()

def CreateDB(db):
    assert _conn
    assert _cur
    _cur.execute('create database if not exists %s character set utf8' % db)
    _conn.commit()

def CreateTable(tabref):
    assert _conn
    assert _cur
    _cur.execute("""
    create table if not exists %s (
        username varchar(32) not null,
        platform enum('xdata', 'hive', 'hdfs') not null default 'xdata',
        object varchar(2048) not null,
        level enum('RECORD', 'ALERT', 'FORBID') not null default 'RECORD',
        attachment varchar(2048) 
    )
    """ % tabref)
    # select * from sensitive_object where platform != '' and level != '' and (upper(level) = 'RECORD' or (attachment is not null and trim(attachment) != ''));
    _conn.commit()

def Release():
    if _cur:
        _cur.close()
        _cur = None
    if _conn:
        _conn.close()
        _conn = None

def main():
    Connect(host='127.0.0.1', port=3309, user='root', passwd='root123', db='mysql', use_unicode=True, charset='utf8')

    CreateDB('strategy_com')
    CreateTable('strategy_com.sensitive_obejct')

    CreateDB('strategy_ebuy')
    CreateTable('strategy_ebuy.sensitive_obejct')

    CreateDB('strategy_eco')
    CreateTable('strategy_eco.sensitive_obejct')

    Release()

if __name__ == '__main__':
    main()

#!/usr/bin/env python

import re
import MySQLdb as mdb
import datetime as dt
import Log

# cur.execute('select * from gv_local.cf_psm_operation_log')

def CellToString(c):
    if isinstance(c, unicode):
        return c
    if '__str__' in dir(c):
        return str(c)
    return repr(c)
    
class StrategyShell:
    def __init__(self):
        self.sql = None
        self.conn = None
        self.conn = mdb.connect(host='127.0.0.1', port=3309,
                                user='root', passwd='root123',
                                db='strategy_ebuy',
                                use_unicode=True, charset='utf8')
        self.logger = None
        self.logger = Log.Log(host='127.0.0.1', port=3309, 
                              user='root', passwd='root123',
                              db='gv_local', 
                              use_unicode=True, charset='utf8')
        assert self.conn
        
    def do_query(self, cur, user, sql):
        try:
            self.conn.commit()
            affected = cur.execute(sql)
            header = '     '.join(map(lambda x: x[0], cur.description))
            segment = ''.zfill(len(header)).replace('0', '-')
            print header
            print segment
            for i in xrange(affected):
                print '\t'.join(map(CellToString, cur.fetchone()))
            print '%d rules.' % affected
            self.logger.info('strategy_ebuy', sql, 'SUCCESS', 'query', user)
        except Exception as e:
            self.logger.warn('strategy_ebuy', sql, 'FAILED', 'query', user)
            print '[EXCEPTION] -- ',
            print e
                
    def do_modify(self, cur, user, sql):
        try:
            affected = cur.execute(sql)
            self.conn.commit()
            print '%d rules affected.' % affected
            self.logger.info('strategy_ebuy', sql, 'SUCCESS', 'modify', user)
        except Exception as e:
            self.logger.warn('strategy_ebuy', sql, 'FAILED', 'modify', user)
            print '[EXCEPTION] -- ',
            print e

    def execute(self, user, sql):
        cur = None
        try:
            cur = self.conn.cursor()
            cmdname = sql.split(None, 1)[0].lower()
            if cmdname == 'select':
                self.do_query(cur, user, sql)
            elif cmdname in ['insert', 'update', 'delete', 'truncate']:
                self.do_modify(cur, user, sql)
            else:
                self.logger.info('strategy_ebuy', sql, 'FAILED', 'unknown', user)
                print '[EXCEPTION] -- unsupported operation -- %s' % sql
        except Exception as e:
            self.logger.warn('strategy_ebuy', sql, 'FAILED', 'unknown', user)
            print '[EXCEPTION] -- ',
            print e
        finally:
            if cur:
                cur.close()
                cur = None
            print

    def __del__(self):
        if self.conn:
            self.conn.close()
            self.conn = None
        return

if __name__ == '__main__':
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    args = sys.stdin.read().lstrip().split(None, 1)
    assert len(args) == 2
    user = args[0]
    sql = "'".join(args[1].split('^'))
    print sql
    StrategyShell().execute(user, sql)

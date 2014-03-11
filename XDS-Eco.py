#!/usr/bin/env python

import codecs
import copy
import getopt
import math
import os
import re
import string
import sys
import unicodedata
import commands
import time

import pdb

import Regex
import Log
import SimpleMail
import Hdfs
import MySQLdb as mdb
import Debug

_username = None
_platform = None
_quest = None

_log = None

_defaultXdataDB = None

_defaultHiveDB = None

_defaultHdfsScheme = 'hdfs'
_defaultHdfsAuthority = '192.168.100.132:8020'
_defaultHdfsHome = 'usr'
_defaultHdfsUser = 'root'

_defaultLogdbHost = '127.0.0.1'
_defaultLogdbPort = 3309
_defaultLogdbUser = 'root'
_defaultLogdbPasswd = 'root123'
_defaultLogdbDB = 'gv_local'
_defaultLogdbUseUnicode = True
_defaultLogdbCharset = 'utf8'

_defaultRuledbHost = '127.0.0.1'
_defaultRuledbPort = 3309
_defaultRuledbUser = 'root'
_defaultRuledbPasswd = 'root123'
_defaultRuledbDB = 'strategy_eco'
_defaultRuledbUseUnicode = True
_defaultRuledbCharset = 'utf8'

# _defaultMailServer = 'smtp.qq.com'
# _defaultMailPort = 25
# _defaultMailUser = 'xds-test@qq.com'
# _defaultMailPasswd = 'xds-test'
 
_defaultMailServer = '192.168.100.225'
_defaultMailPort = 25
_defaultMailUser = 'xdata@z152.com'
_defaultMailPasswd = 'xdata'

_defaultXDSRoot = '/root/XData4S'
# _defaultJavaRoot = '/usr/java/jdk1.6.0_43'
_defaultJavaRoot = '/usr/lib/xdata/jdk'

_defaultHiveRoot = '/usr/lib/xdata/hive'
_defaultHdfsRoot = '/usr/lib/xdata/hdfs'
_defaultHadoopRoot = '/usr/lib/xdata/mapreduce'

_XDSXqe = 'XDSXqe'
_XDSHql = 'XDSHql.jar'
_XDSXql = 'XDSXql'
_JAVA = 'java'
_HIVE = 'hive'
_HDFS = 'hdfs'
_HADOOP = 'hadoop'

_newlinePattern = re.compile(r'\s*\r?\n')

class XdsError(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

def GetArguments():
    global _username
    global _platform
    global _quest
    argline = "'".join(sys.stdin.read().strip().split('^'))
    arguments = argline.split(None, 2)
    # print 'GetArguments/arguemnts: ' + str(arguments)
    if not arguments or len(arguments) != 3:
        raise XdsError('Logic Error -- insufficient arguments')
    _username, _platform, _quest = arguments
    _platform = _platform.lower()
    _quest = _quest.strip()
    return

def GetObjects(objstr):
    rawobjs = objstr.strip().split()
    objlist = [x.strip().lower() for x in rawobjs]
    return set(objlist)

def HandleCommandOutput(outtext):
    lines = filter(None, _newlinePattern.split(outtext.strip()))
    objects = set()
    for line in lines:
        line = line.strip()
        Debug.Print(line)
        if line.startswith('[OBJECT]'):
            objects = objects | GetObjects(line[len('[OBJECT]'):])
        elif line.startswith('[EXCEPTION]'):
            raise XdsError('Exception -- ' + line[len('[EXCEPTION]'):].strip())
        else:
            raise XdsError('Logic Error -- ' + line)
    return objects
    
def RunThenGetOuttext(cmd):
    exitstatus, outtext = commands.getstatusoutput(cmd)
    if exitstatus != 0:
    	Debug.Print(outtext)
        raise XdsError('Failed -- %s : %s' % (_quest, outtext))
    return outtext

def RunThenGetOperands(cmd):
    outtext = RunThenGetOuttext(cmd)
    if not outtext or not outtext.strip():
        raise XdsError('Parsed Nothing -- ' + cmd)
    operands = HandleCommandOutput(outtext.strip())
    return operands

def GetXdataOperands():
    cmd = 'echo "%s" | %s' % (_quest, _XDSXql)
    operands = RunThenGetOperands(cmd)
    return operands

def GetHiveOperands():
    cmd = 'echo "%s" | %s -jar %s' % (_quest, _JAVA, _XDSHql)
    Debug.Print('GetHiveOperands/cmd: ' + cmd)
    operands = RunThenGetOperands(cmd)
    return operands

def GetHdfsOperands():
    return Hdfs.GetHdfsUris(_defaultHdfsScheme, _defaultHdfsAuthority, _defaultHdfsHome, _defaultHdfsUser, _quest)

def GetRules(**kwargs):
    conn = None
    cur = None
    try:
        conn = mdb.connect(**kwargs)
        cur = conn.cursor()
        nrows = cur.execute("""
        select distinct object, level, attachment
        from sensitive_object
        where 
          trim(platform) != '' and 
          trim(level) != '' and
          (upper(level) = 'RECORD' or
            (attachment is not null and trim(attachment) != '')) and
          lower(username) = %s and lower(platform) = %s
        """, (_username.strip().lower(), _platform.strip().lower()))
        if nrows:
            return cur.fetchall()
        else:
            return tuple()
    finally:
        if cur:
            cur.close()
            cur = None
        if conn:
            conn.close()
            conn = None

def GetLowercaseDatabaseTablePair(dbtab):
    assert _platform in ['hive', 'xdata']
    assert dbtab and dbtab.strip()
    dbtab = dbtab.strip().lower()
    if '.' not in dbtab:
        return GetDefaultDB(), dbtab
    if dbtab[0] == '.' or dbtab[-1] == '.' or dbtab.count('.') != 1:
        raise XdsError('Logic Error -- invalid table reference %s' % dbtab)
    db, tab = dbtab.split('.')
    return db, tab
    
def AreRelativeTables(ee, er):
    eedb, eetab = GetLowercaseDatabaseTablePair(ee)
    erdb, ertab = GetLowercaseDatabaseTablePair(er)
    return eedb == erdb and eetab == ertab
        
def AreRelativePathes(ee, er):
    return Hdfs.HasIntersected(_defaultHdfsScheme, _defaultHdfsAuthority, _defaultHdfsHome, _defaultHdfsUser, ee, er)

def AreRelativeObjects(ee, er):
    if _platform == 'hive' or _platform == 'xdata':
        return AreRelativeTables(ee, er)
    else:
        return AreRelativePathes(ee, er)

def GetActions(objects, rules):
    acts = []
    for ee in objects:
        for er, level, attachment in rules:
            if AreRelativeObjects(ee, er):
                acts.append((ee, er, level, attachment))
    return acts
        
def TakeAction(action):
    ee, er, level, attachment = action
    level = level.upper()

    assert level in ['RECORD', 'ALERT', 'FORBID']

    print '[%s] -- %s ON %s WITH %s VS %s WHILE TRYING {%s}' % (level, _username, _platform, ee, er,  _quest)

    _log.info(_platform, _quest, 'LOG', 'SENSITIVE_ECO', _username)

    if not level or level == 'RECORD':
        _log.info(_platform, _quest, level, 'SENSITIVE_ECO', _username)
        canGoOn = True
        return canGoOn

    tolist = [i.strip() for i in attachment.strip().split(',') if i.strip()]
    if not tolist or not len(tolist):
        raise XdsError('Logic Error -- invalid attachment "%s"' % attachment)
    subject = 'XDS-ALERT' if level == 'ALERT' else 'XDS-FORBID'
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    content = '''
    username:       %s
    to-visit:       %s
    platform:       %s
    timestamp:      %s
    rule:           %s
    quest:          %s
    ''' % (_username, ee, _platform, timestamp, er, _quest)
    _mailer.sendMail(tolist, subject, content)
    if level == 'ALERT':
        _log.warn(_platform, _quest, level, 'SENSITIVE_ECO', _username)
        canGoOn = True
    else:
        _log.error(_platform, _quest, level, 'SENSITIVE_ECO', _username)
        canGoOn = False
    return canGoOn

def GetOperands():
    assert _platform in ['hive', 'hdfs', 'xdata'] and _quest and _quest.strip()
    Debug.Print('GetOperands/_platform: ' + _platform)
    if _platform == 'xdata':
        operands = GetXdataOperands()
    elif _platform == 'hdfs':
        operands = GetHdfsOperands()
    else:
        operands = GetHiveOperands()
    Debug.Print('Operands: %s' % str(operands))
    return operands

def AssembleHadoopCommand():
    assert _defaultHadoopRoot and _defaultHadoopRoot.strip()
    assert _quest and _quest.strip()
    quest = _quest.strip()
    cmd = ""
    if quest.startswith("hdfs"):
        cmd = "%s fs -%s" % (_HADOOP, quest[len("hdfs."):])
    elif quest.startswith("hdjar"):
        cmd = "%s jar %s/hadoop-examples-*.jar %s" % (_HADOOP, _defaultHadoopRoot, quest[len("hdjar."):])
    else:
        raise HdfsError("Invalid Quest -- " + quest)
    return cmd
        
def AssemblePlatformCommand():
    assert _platform in ['hive', 'xdata', 'hdfs'] and _quest and _quest.strip()
    cmd = ''
    if _platform == 'xdata':
        # cmd = '''echo "%s" | %s /tmp/XDSXqe.re &>/dev/null && cat /tmp/XDSXqe.res''' % (_quest, _XDSXqe)
        cmd = '''echo "%s" | %s''' % (_quest, _XDSXqe)
    elif _platform == 'hive':
        cmd = _HIVE + " << EOF\n" + _quest + '\n' + 'EOF'
        cmd = """echo "%s" | sh""" % cmd
        Debug.Print(cmd)
    else:
        # cmd = _defaultHdfsRoot + '/bin/' + _quest.strip()
        cmd = AssembleHadoopCommand()
    assert cmd and cmd.strip()
    return cmd

def LetGo():
    assert _platform in ['hive', 'xdata', 'hdfs'] and _quest and _quest.strip()
    cmd = AssemblePlatformCommand()
    outtext = RunThenGetOuttext(cmd)
    return outtext
    # if _platform == 'xdata':
    #     return unicode(outtext, "gb2312").encode("utf8")
    # else:
    #     # return unicode(outtext, "utf8").encode("utf8")
    #     return outtext

def GetDefaultDB():
    assert _platform in ['hive', 'xdata']
    if _platform == 'hive':
        return _defaultHiveDB
    else:
        return _defaultXdataDB

def SetOptions():
    path = _defaultXDSRoot + '/' + 'XDS.rc'
    if not os.path.exists(path) or not os.path.isfile(path):
        return

    global _defaultXdataDB
    global _defaultHiveDB
    global _defaultHdfsScheme
    global _defaultHdfsAuthority
    global _defaultHdfsHome
    global _defaultLogdbHost
    global _defaultLogdbPort
    global _defaultLogdbUser
    global _defaultLogdbPasswd
    global _defaultLogdbDB
    global _defaultLogdbUseUnicode
    global _defaultLogdbCharset
    global _defaultRuledbHost
    global _defaultRuledbPort
    global _defaultRuledbUser
    global _defaultRuledbPasswd
    global _defaultRuledbDB
    global _defaultRuledbUseUnicode
    global _defaultRuledbCharset
    global _defaultMailServer
    global _defaultMailPort
    global _defaultMailUser
    global _defaultMailPasswd

    with open(path) as input:
        for line in input.readlines():
            linefields = line.strip().split()
            if not linefields or len(linefields) != 2:
                continue
            elif linefields[0] == 'HIVE_DB':
                _defaultHiveDB = linefields[1]
            elif linefields[0] == 'XDATA_DB':
                _defaultXdataDB = linefields[1]
            elif linefields[0] == 'HDFS_SCHEME':
                _defaultHdfsScheme = linefields[1]
            elif linefields[0] == 'HDFS_AUTHORITY':
                _defaultHdfsAuthority = linefields[1]
            elif linefields[0] == 'HDFS_HOME':
                _defaultHdfsHome = linefields[1]
            elif linefields[0] == 'LOGDB_HOST':
                _defaultLogdbHost = linefields[1]
            elif linefields[0] == 'LOGDB_PORT':
                _defaultLogdbPort = int(linefields[1])
            elif linefields[0] == 'LOGDB_USER':
                _defaultLogdbUser = linefields[1]
            elif linefields[0] == 'LOGDB_PASSWD':
                _defaultLogdbPasswd = linefields[1]
            elif linefields[0] == 'LOGDB_DB':
                _defaultLogdbDB = linefields[1]
            elif linefields[0] == 'LOGDB_USE_UNICODE':
                _defaultLogdbUseUnicode = (linefields[1] == 'True')
            elif linefields[0] == 'LOGDB_CHARSET':
                _defaultLogdbCharset = linefields[1]
            elif linefields[0] == 'RULEDB_HOST':
                _defaultRuledbHost = linefields[1]
            elif linefields[0] == 'RULEDB_PORT':
                _defaultRuledbPort = int(linefields[1])
            elif linefields[0] == 'RULEDB_USER':
                _defaultRuledbUser = linefields[1]
            elif linefields[0] == 'RULEDB_PASSWD':
                _defaultRuledbPasswd = linefields[1]
            elif linefields[0] == 'RULEDB_DB':
                _defaultRuledbDB = linefields[1]
            elif linefields[0] == 'RULEDB_USE_UNICODE':
                _defaultRuledbUseUnicode = (linefields[1] == 'True')
            elif linefields[0] == 'RULEDB_CHARSET':
                _defaultRuledbCharset = linefields[1]
            elif linefields[0] == 'MAIL_SERVER':
                _defaultMailServer = linefields[1]
            elif linefields[0] == 'MAIL_PORT':
                _defaultMailPort = int(linefields[1])
            elif linefields[0] == 'MAIL_USER':
                _defaultMailUser = linefields[1]
            elif linefields[0] == 'MAIL_PASSWD':
                _defaultMailPasswd = linefields[1]
            else:
                continue
    return
        
def GetEnv():
    global _defaultXDSRoot
    xdsRootEnv = os.environ.get('XDS_ROOT')
    if xdsRootEnv:
        _defaultXDSRoot = xdsRootEnv

    global _defaultJavaRoot
    JavaRootEnv = os.environ.get('Java_ROOT')
    if JavaRootEnv:
    	_defaultJavaRoot = JavaRootEnv

    global _defaultHiveRoot
    HiveRootEnv = os.environ.get('Hive_ROOT')
    if HiveRootEnv:
    	_defaultHiveRoot = HiveRootEnv

    global _defaultHdfsRoot
    HdfsRootEnv = os.environ.get('Hdfs_ROOT')
    if HdfsRootEnv:
    	_defaultHdfsRoot = HdfsRootEnv

    global _defaultHadoopRoot
    HadoopRootEnv = os.environ.get('Hadoop_ROOT')
    if HadoopRootEnv:
        _defaultHadoopRoot = HadoopRootEnv
        
    global _defaultHdfsUser
    DefaultHdfsUserEnv = os.environ.get('USER')
    Debug.Print('GetEnv/DefaultHdfsUserEnv: ' + DefaultHdfsUserEnv)
    if DefaultHdfsUserEnv:
        _defaultHdfsUser = DefaultHdfsUserEnv

def SetBinary():
    global _XDSXql
    global _XDSXqe
    global _XDSHql
    global _JAVA
    global _HIVE
    global _HDFS
    global _HADOOP
    _XDSXql = _defaultXDSRoot + '/' + 'bin/XDSXql'
    _XDSXqe = _defaultXDSRoot + '/' + 'bin/XDSXqe'
    _XDSHql = _defaultXDSRoot + '/' + 'bin/XDSHql.jar'
    _JAVA = _defaultJavaRoot + '/' + 'bin/java'
    _HIVE = _defaultHiveRoot + '/' + 'bin/hive'
    _HDFS = _defaultHdfsRoot + '/' + 'bin/hdfs'
    _HADOOP = _defaultHadoopRoot + '/' + 'bin/hadoop'
    
def Ikuzo():
    global _log
    global _mailer
    GetEnv()
    SetOptions()
    SetBinary()
    _mailer = SimpleMail.SimpleMail(server=_defaultMailServer,
                                    port=_defaultMailPort,
                                    user=_defaultMailUser,
                                    passwd=_defaultMailPasswd)
    _log = Log.Log(host=_defaultLogdbHost,
                  port=_defaultLogdbPort,
                  user=_defaultLogdbUser,
                  passwd=_defaultLogdbPasswd,
                  db=_defaultLogdbDB,
                  use_unicode=_defaultRuledbUseUnicode,
                  charset=_defaultLogdbCharset)
    GetArguments()
    operands = GetOperands()
    Debug.Print('ikuzo/operands: ' + str(operands))
    rules = GetRules(host=_defaultRuledbHost,
                     port=_defaultRuledbPort,
                     user=_defaultRuledbUser,
                     passwd=_defaultRuledbPasswd,
                     db=_defaultRuledbDB,
                     use_unicode=_defaultRuledbUseUnicode,
                     charset=_defaultRuledbCharset)
    Debug.Print('ikuzo/rules: ' + str(rules))
    actions = GetActions(operands, rules)
    Debug.Print('ikuzo/actions: ' + str(actions))
    canGoOn = True
    for action in actions:
        canGoOn = (canGoOn and TakeAction(action))
    output = '''--------------------------------------------------\n'''
    if canGoOn:
        gores = LetGo()
        output = output + gores
    print output

def Trace():
    Debug.NDEBUG = False
    Ikuzo()

def Fighting():
    try:
        Debug.NDEBUG = True
        Ikuzo()
    except XdsError as e:
        print '[EXCEPTION] -- %s' % e.message
    except Hdfs.HdfsError as e:
        print '[EXCEPTION] -- %s' % e.message
    except Exception as e:
        print '[EXCEPTION] -- ',
        print e

if __name__ == '__main__':
    import sys 
    reload(sys) 
    sys.setdefaultencoding('utf8')
    if len(sys.argv) > 1 and sys.argv[1].lower() == '-d':
        Trace()
    else:
        Fighting()

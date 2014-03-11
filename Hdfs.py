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
import pdb

import Regex
import Debug

_nonWordRegex = r'\W+'

_nonSpaceRegex = r'\S+'

_spaceRegex = r'\s+'

_newlineRegex = r'\s*\r?\n'

# _hdfsCommandRegex = r"""(?P<hdfsCommand>
# \s*
# (?: (?P<hdfs> hdfs) | hadoop)
# \s*
# (?P<confDir> --config \s+ .+?)?
# \s*
# \b (?(hdfs) dfs | fs) \b
# \s*
# (?P<genericOptions> .+?)?
# \s*
# - (?P<kind>
#     cat | chgrp | chmod | chown |
#     copyFromLocal | copyToLocal | count |
#     cp | df | du | expunge | get | getmerge |
#     ls | mkdir | moveFromLocal | moveToLocal |
#     mv | put | rm | rmdir | setrep | stat |
#     tail | test | text | touchz | usage
# )
# \s*
# (?P<concern> .+)
# )
# """

# _mapreduceRegex = r"""(?P<hadoopJar>
# \s*
# mapreduce
# \s+
# (?P<kind>
#     aggregatewordcount | aggregatewordhist | grep |
#     multifilewc | randomtextwriter | randomwriter |
#     secondarysort | wordcount
# )
# \s*
# (?P<concern> .+)
# )
# """

_hdfsCommandRegex = r"""(?P<hdfsCommand>
\s*
(?P<domain> (?P<hdfs> hdfs) | hdjar)
\.
(?P<kind>
    (?(hdfs) (
        appendToFile | cat | chgrp | chmod | chown |
        copyFromLocal | copyToLocal | count |
        cp | df | du | expunge | get | getmerge |
        ls | mkdir | moveFromLocal | moveToLocal |
        mv | put | rm | rmdir | setrep | stat |
        tail | test | text | touchz | usage
    ) | (
        aggregatewordcount | aggregatewordhist | grep |
        multifilewc | randomtextwriter | randomwriter |
        secondarysort | wordcount
    ))
)
\s*
(?P<concern> .+)
)
"""

# _rawUriRegex = r"""(?:
# (?: (hdfs | file) :// [^/]* (?= /))?
# /?
# ((?! /) (?: /(?!/) | [^:/])*)
# )
# """
_rawUriRegex = r"""(?:
(?: (?: hdfs | file) :// (?: [^\s/]*) (?= /))?
(?: /)?
(?: (?! /) (?: /(?!/) | [^:/\s])*)
)
"""
_uriRegex = r"""(?P<uri>
(?: (?P<scheme> hdfs | file) :// (?P<authority> [^\s/]*) (?= /))?
(?P<root> /)?
(?P<relativePath> (?! /) (?: /(?!/) | [^:/\s])*)
)
"""
_urisRegex = r"""(?P<uris>
%s (\s* %s)*?                   # CAUTIONS: lazy matching
)
""" % (_rawUriRegex, _rawUriRegex)

# _rawHdfsUriRegex = r"""(?:
# (?: hdfs :// [^/]* (?= /))?
# /?
# ((?! /) (?: /(?!/) | [^:/])*)
# )
# """
_rawHdfsUriRegex = r"""(?:
(?: (?: hdfs) :// (?: [^\s/]*) (?= /))?
(?: /)?
(?: (?! /) (?: /(?!/) | [^:/\s])*)
)
"""
_hdfsUriRegex = r"""(?P<hdfsUri>
(?: (?P<scheme> hdfs) :// (?P<authority> [^\s/]*) (?= /))?
(?P<root> /)?
(?P<relativePath> (?! /) (?: /(?!/) | [^:/\s])*)
)
"""
_hdfsUrisRegex = r"""(?P<hdfsUris>
%s (\s* %s)*?                   # CAUTIONS: lazy matching
)
""" % (_rawHdfsUriRegex, _rawHdfsUriRegex)

# _rawLocalUriRegex = r"""
# (?: file :// [^/]* (?= /))?
# /?
# ((?! /) (?: /(?!/) | [^:/])*)
# """
_rawLocalUriRegex = r"""(?:
(?: (?: file) :// (?: [^\s/]*) (?= /))?
(?: /)?
(?: (?! /) (?: /(?!/) | [^:/\s])*)
)
"""
_localUriRegex = r"""(?P<localUri>
(?: (?P<scheme> file) :// (?P<authority> [^\s/]*) (?= /))?
(?P<root> /)?
(?P<relativePath> (?! /) (?: /(?!/) | [^:/\s])*)
)
"""
_localUrisRegex = r"""(?P<localUris>
%s (\s* %s)*?                   # CAUTIONS: lazy matching
)
""" % (_rawLocalUriRegex, _rawLocalUriRegex)

_rawModeRegex = r"""(?:
[ugoa]*
\s*
[+-=]
\s*
(?: [ugo] | [rwxXst]*)          # CAUTIOINS!
)
"""
_modeRegex = r"""(?P<mode>
[ugoa]*
\s*
[+-=]
\s*
(?: [ugo] | [rwxXst]*)          # CAUTIOINS!
)"""
_modesRegex = r"""(?P<modes>
%s (\s* , \s* %s)*?
)
""" % (_rawModeRegex, _rawModeRegex)

_rawIdRegex = r"""(?:
\w+
)
"""
_idRegex = r"""(?P<id>
\w+                             # TODO(gongxidong): maybe
)
"""

_octalmodeRegex = r"""(?P<octalmode>
[0-7]{1, 4}
)
"""

_appendToFileRegex = r"""(?P<appendToFile>
( - | %s)                       # localUris
\s*
%s                              # uri
)
""" % (_localUrisRegex, _uriRegex)

_catRegex = r"""(?P<cat>
%s                              # uris
)
""" % (_urisRegex)

_chgrpRegex = r"""(?P<chgrp>
(-R)?
\s*
%s                              # id
\s*
%s                              # uris
)
""" % (_idRegex, _urisRegex)

_chmodRegex = r"""(?P<chmod>
(-R)?
\s*
(%s | %s)                       # modes octalmode
\s*
%s                              # uris
)
""" % (_modesRegex, _octalmodeRegex, _urisRegex)

_chownRegex = r"""(?P<chown>
(-R)?
\s*
%s? (\s* : \s* %s?)?            # user group
\s*
%s                              # uris
)
""" % (_rawIdRegex, _rawIdRegex, _urisRegex)

_copyFromLocalRegex = r"""(?P<copyFromLocal>
(-f)?
\s*
%s                              # localUri
\s*
%s                              # uri
)
""" % (_localUriRegex, _uriRegex)

_copyToLocalRegex = r"""(?P<copyToLocal>
(-ignorecrc)?
\s*
(-crc)?
\s*
%s                              # uri
\s*
%s                              # localUri
)
""" % (_uriRegex, _localUriRegex)

_countRegex = r"""(?P<count>
(-q)?
\s*
%s                              # uris
)
""" % _urisRegex

_cpRegex = r"""(?P<cp>
(-f)?
\s*
%s                              # uris
\s*
%s                              # uri
)
""" % (_urisRegex, _uriRegex)

_duRegex = r"""(?P<du>
(-s)?
\s*
(-h)?
\s*
%s                              # uris
)
""" % _urisRegex

_dusRegex = r"""(?P<dus>
(-h)?
\s*
%s                              # uris
)
""" % _urisRegex

_expungeRegex = r"""(?P<expunge>
                                # CAUTIONS!
)
"""

_getRegex = r"""(?P<get>
(-ignorecrc)?
\s*
(-crc)?
\s*
%s                              # uris
\s*
%s                              # localUri
)
""" % (_urisRegex, _localUriRegex)

_getmergeRegex = r"""(?P<getmerge>
%s                              # uri
\s*
%s                              # localUri
\s*
(addnl)?
)
""" % (_urisRegex, _localUriRegex)

_lsRegex = r"""(?P<ls>
%s                              # uris
)
""" % _urisRegex

_lsrRegex = r"""(?P<lsr>
%s                              # uris
)
""" % _urisRegex

_mkdirRegex = r"""(?P<mkdir>
(-p)?
\s*
%s                              # uris
)
""" % _urisRegex

_moveFromLocalRegex = r"""(?P<moveFromLocal>
%s                              # localUris
\s*
%s                              # uri --> maybe hdfsUri
)
""" % (_localUrisRegex, _uriRegex)

_moveToLocalRegex = r"""(?P<moveToLocal>
(-crc)?
\s*
%s                              # uris --> maybe hdfsUris
\s*
%s                              # localUri
)
""" % (_urisRegex, _localUriRegex)

# _mvRegex = r"""(?P<mv>
# (?P<hdfs> %s \s* %s)            # hdfsUris hdfsUri
# |
# (?P<local> %s \s* %s)           # localUris localUri
# )
# """ % (_hdfsUrisRegex, _hdfsUriRegex, _localUrisRegex, _localUriRegex)

_mvRegex = r"""(?P<mv>
(?P<hdfs> %s \s* %s)            # hdfsUris hdfsUri
|
(?P<local> %s \s* %s)           # localUris rawLocalUri(CAUTIONS: inconsistency, but dont worry, local* just so so)
)
""" % (_hdfsUrisRegex, _hdfsUriRegex, _localUrisRegex, _rawLocalUriRegex)

_putRegex = r"""(?P<put>
(- | %s)                        # localUris
\s*
%s                              # uri
)
""" % (_localUrisRegex, _uriRegex)

_rmRegex = r"""(?P<rm>
(-skipTrash)?
\s*
%s                              # uris
)
""" % _urisRegex

_rmrRegex = r"""(?P<rmr>
(-skipTrash)?
\s*
%s                              # uris
)
""" % _urisRegex

_positiveIntegerRegex = r"""(?P<positiveInteger>
[1-9]\d*
)
"""

_setrepRegex = r"""(?P<setrep>
(-R)?
\s*
(-w)?
\s*
%s                              # positiveInteger
\s*
%s                              # hdfsUri
)
""" % (_positiveIntegerRegex, _hdfsUriRegex)

_statRegex = r"""(?P<stat>
%s                              # uris
)
""" % _urisRegex

_tailRegex = r"""(?P<tail>
(-f)?
\s*
%s                              # uri
)
""" % _uriRegex

_testRegex = r"""(?P<test>
-[ezd]
\s*
%s                              # uri
)
""" % _uriRegex

_textRegex = r"""(?P<text>
%s                              # uri
)
""" % _uriRegex

_touchzRegex = r"""(?P<touchz>
%s                              # uris
)
""" % _urisRegex

_aggregatewordcountRegex = r"""(?P<aggregatewordcount>
%s                              # uris
\s*
%s                              # uri
)
""" % (_urisRegex, _uriRegex)

_aggregatewordhistRegex = r"""(?P<aggregatewordhist>
%s                              # uris
\s*
%s                              # uri
)
""" % (_urisRegex, _uriRegex)

_regexRegex = r"""(?P<regex>
(?P<qm> ['"])
(?P<quote> (?: \\['"] | [^'"])*?)
(?P=qm)
)
"""

_grepRegex = r"""(?P<grep>
(?P<uris> %s \s* %s)
\s*
%s                              # regex
\s*
(?: (?: (?P<qmgroup> ['"]) \w+ (?P=qmgroup)) | \w+)? # group
# (?: (?P<qmgroup> ['"])? \w+ (?(qmgroup)(?=qmgroup)))?  # group
)
""" % (_rawUriRegex, _rawUriRegex, _regexRegex)

_multifilewcRegex = r"""(?P<multifilewc>
(?P<uris> %s \s* %s)
)
""" % (_rawUriRegex, _rawUriRegex)

_randomtextwriterRegex = r"""(?P<randomtextwriter>
(?: -outFormat \s* (\w|\.)+)?   # [-outFormat <outputFormatClass>]
\s*
%s                              # uri
)
""" % _uriRegex

_randomwriterRegex = r"""(?P<randomwriter>
%s                              # uri
)
""" % _uriRegex

_secondarysortRegex = r"""(?P<secondarysort>
(?P<uris> %s \s* %s)
)
""" % (_rawUriRegex, _rawUriRegex)

_wordcountRegex = r"""(?P<wordcount>
(?P<uris> %s \s* %s)
)
""" % (_rawUriRegex, _rawUriRegex)

class HdfsError(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

def __NormalizeHdfsUri(defaultScheme, defaultAuthority, defaultHome, username, uri):
    assert defaultScheme and defaultScheme.strip()
    assert defaultAuthority and defaultAuthority.strip()
    assert (defaultHome and defaultHome.strip()) or (username and username.strip())
    assert defaultHome.strip()[0] != '/' and defaultHome.strip()[-1] != '/'
    assert username and username.strip()
    assert uri and uri.strip()

    matched = Regex.Match(Regex.WrapArroundRegex(_hdfsUriRegex), uri)
    if not matched:
        raise HdfsError('Invalid hdfs uri -- ' + uri)
    scheme = matched.group('scheme')
    authority = matched.group('authority')
    root = matched.group('root')
    relativePath = matched.group('relativePath')
    if not (scheme or authority or root or relativePath):
        raise HdfsError('Invalid hdfs uri -- ' + uri)

    path = ''

    if scheme:
        path = path + scheme
    else:
        path = path + defaultScheme

    path = path + '://'

    if authority:
        path = path + authority
    else:
        path = path + defaultAuthority

    if root:
        path = path + root
    elif defaultHome and defaultHome.strip() and username and username.strip():
        path = path + '/' + defaultHome + '/' + username + '/'
    else:       # elif not defaultHome or not defaultHome.strip():
        path = path + '/' + username + '/'

    if relativePath:
        path = path + relativePath

    return path

###
# make sure the commandName is the corresponding regex's group name
# make sure the necessary regex's group names in _decompositionMap
#
_decompositionMap = {
    'uris': _rawUriRegex,
    'hdfsUris': _rawHdfsUriRegex,
    'uri': None,
    'hdfsUri': None
    }
_commandBoosterMap = {
    'appendToFile':     (_appendToFileRegex,    ['uri'],                []),
    'cat':              (_catRegex,             ['uris'],               []),
    'chgrp':            (_chgrpRegex,           ['uris'],               []),
    'chmod':            (_chmodRegex,           ['uris'],               []),
    'chown':            (_chownRegex,           ['uris'],               []),
    'copyFromLocal':    (_copyFromLocalRegex,   ['uri'],                []),
    'copyToLocal':      (_copyToLocalRegex,     ['uris'],               []),
    'count':            (_countRegex,           ['uris'],               []),
    'cp':               (_cpRegex,              ['uris', 'uri'],        []),
    'du':               (_duRegex,              ['uris'],               []),
    'dus':              (_dusRegex,             ['uris'],               []),
    'expunge':          (_expungeRegex,         [],                     []),
    'get':              (_getRegex,             ['uris'],               []),
    'getmerge':         (_getmergeRegex,        ['uri'],                []),
    'ls':               (_lsRegex,              ['uris'],               []),
    'lsr':              (_lsrRegex,             ['uris'],               []),
    'mkdir':            (_mkdirRegex,           ['uris'],               []),
    'moveFromLocal':    (_moveFromLocalRegex,   ['uri'],                []),
    'moveToLocal':      (_moveToLocalRegex,     ['uris'],               []),
    'mv':               (_mvRegex,              [],                     ['hdfsUris', 'hdfsUri']),
    'put':              (_putRegex,             ['uri'],                []),
    'rm':               (_rmRegex,              ['uris'],               []),
    'rmr':              (_rmrRegex,             ['uris'],               []),
    'setrep':           (_setrepRegex,          ['hdfsUri'],            []),
    'stat':             (_statRegex,            ['uris'],               []),
    'tail':             (_tailRegex,            ['uri'],                []),
    'test':             (_testRegex,            ['uri'],                []),
    'text':             (_textRegex,            ['uri'],                []),
    'touchz':           (_touchzRegex,          ['uris'],               []),
    # 
    'aggregatewordcount': (_aggregatewordcountRegex, ['uris', 'uri'],   []),
    'aggregatewordhist': (_aggregatewordhistRegex, ['uris', 'uri'],     []),
    'grep':             (_grepRegex,            ['uris'],               []),
    'multifilewc':      (_multifilewcRegex,     ['uris'],               []),
    'randomtextwriter': (_randomtextwriterRegex, ['uri'],               []),
    'randomwriter':     (_randomwriterRegex,    ['uri'],                []),
    'secondarysort':    (_secondarysortRegex,   ['uris'],               []),
    'wordcount':        (_wordcountRegex,       ['uris'],               []),
    }

def __Decomposite(searched, groupNames):
    uris = []
    if not searched or not groupNames:
        return uris
    for groupName in groupNames:
        grouped = searched.group(groupName)
        if not grouped:
            raise HdfsError(groupName) # CAUTION: non-consistant
        decompositionRegex = _decompositionMap[groupName]
        if decompositionRegex:
            decompositionPattern = Regex.LazyCompile(decompositionRegex)
            rawElements = decompositionPattern.findall(grouped)
            Debug.Print("__Decomposite/rawElements: %s" % str(rawElements))
            elements = filter(None, rawElements)
            Debug.Print("__Decomposite/elements: %s" % str(elements))
            uris.extend(elements)
        else:
            uris.append(grouped)
    return uris

def __GetUris(commandName, concern):
    commandBooster = _commandBoosterMap[commandName]
    if not commandBooster:
        raise HdfsError('Invalid "%s" Quest -- %s'  % commandName)
    commandRegex, determinaterGroupNames, alternativeGroupNames = commandBooster
    Debug.Print("__GetUris/commandRegex: %s" % commandRegex)
    searched = Regex.Search(Regex.WrapArroundRegex(commandRegex), concern)
    Debug.Print("__GetUris/searched.groups() == %s" % str(searched.groups()))
    if not searched or not searched.group(commandName):
        raise HdfsError('Invalid "%s" Quest -- %s match %s error' % (commandName, commandRegex, concern))
    uris = []
    try:
        uris.extend(__Decomposite(searched, determinaterGroupNames))
        Debug.Print("__GetUris/uris: %s" % str(uris))
        uris.extend(__Decomposite(searched, alternativeGroupNames))
        Debug.Print("__GetUris/uris: %s" % str(uris))
    except HdfsError as e:
        raise HdfsError('Invalid "%s" Quest -- group("%s") == None on %s'
                % (commandName, e.message, concern))
    return uris

def __IsLocalUri(uri):
    if uri and Regex.HasMatched(Regex.WrapArroundRegex(_localUriRegex), uri):
        return True
    return False

def __IsHdfsUri(uri):
    if uri and Regex.HasMatched(Regex.WrapArroundRegex(_hdfsUriRegex), uri):
        return True
    return False 

def GetHdfsUris(defaultScheme, defaultAuthority, defaultHome, username, quest):
    matched = Regex.Match(_hdfsCommandRegex, quest)
    if not matched:
        raise HdfsError("Logic Error -- invalid hdfs shell command")
    kind = matched.group('kind').strip()
    concern = matched.group('concern').strip()
    uris = __GetUris(kind, concern)
    hdfsuris = [uri for uri in uris if __IsHdfsUri(uri)]
    normalizeHdfsUris = [__NormalizeHdfsUri(defaultScheme, defaultAuthority,
                                            defaultHome, username, i)
                         for i in hdfsuris]
    return normalizeHdfsUris

def HasIntersected(defaultScheme, defaultAuthority, defaultHome, username, ee, er):
    ee = __NormalizeHdfsUri(defaultScheme, defaultAuthority, defaultHome, username, ee)
    Debug.Print('Hdfs.HasIntersected/ee: ' + ee)
    er = __NormalizeHdfsUri(defaultScheme, defaultAuthority, defaultHome, username, er)
    Debug.Print('Hdfs.HasIntersected/er: ' + er)
    return ee == er or ee.startswith(er) or er.startswith(ee)

__testcases = [{
    'usage': 'hdfs.appendToFile <localsrc> ... <dst>',
    'quests': ['hdfs.appendToFile localfile /user/hadoop/hadoopfile',
               'hdfs.appendToFile localfile1 localfile2 /user/hadoop/hadoopfile',
               'hdfs.appendToFile localfile hdfs://nn.example.com/hadoop/hadoopfile',
               'hdfs.appendToFile - hdfs://nn.example.com/hadoop/hadoopfile']
}, {
    'usage': 'hdfs.cat URI [URI ...]',
    'quests': ['hdfs.cat hdfs://nn1.example.com/file1 hdfs://nn2.example.com/file2',
               'hdfs.cat file:///file3 /user/hadoop/file4']
}, {
    'usage': 'hdfs.chgrp [-R] GROUP URI [URI ...]',
    'quests': None,
}, {
    'usage': 'hdfs.chmod [-R] <MODE[,MODE]... | OCTALMODE> URI [URI ...]',
    'quests': None,
}, {
    'usage': 'hdfs.chown [-R] [OWNER][:[GROUP]] URI [URI ]',
    'quests': None,
}, {
    'usage': 'hdfs.copyFromLocal <localsrc> URI',
    'quests': None,
}, {
    'usage': 'hdfs.copyToLocal [-ignorecrc] [-crc] URI <localdst>',
    'quests': None,
}, {
    'usage': 'hdfs.count [-q] <paths> ',
    'quests': ['hdfs.count hdfs://nn1.example.com/file1 hdfs://nn2.example.com/file2',
               'hdfs.count -q hdfs://nn1.example.com/file1'],
}, {
    'usage': 'hdfs.cp [-f] URI [URI ...] <dest> ',
    'quests': ['hdfs.cp /user/hadoop/file1 /user/hadoop/file2',
               'hdfs.cp /user/hadoop/file1 /user/hadoop/file2 /user/hadoop/dir'],
}, {
    'usage': 'hdfs.du [-s] [-h] URI [URI ...]',
    'quests': ['hdfs.du /user/hadoop/dir1 /user/hadoop/file1 hdfs://nn.example.com/user/hadoop/dir1'],
}, {
    'usage': 'hdfs.dus <args>',
    'quests': None,
}, {
    'usage': 'hdfs.expunge',
    'quests': None,
}, {
    'usage': 'hdfs.get [-ignorecrc] [-crc] <src> <localdst> ',
    'quests': ['hdfs.get /user/hadoop/file localfile',
               'hdfs.get hdfs://nn.example.com/user/hadoop/file localfile'],
}, {
    'usage': 'hdfs.getmerge <src> <localdst> [addnl]',
    'quests': None,
}, {
    'usage': 'hdfs.ls <args> ',
    'quests': None,
}, {
    'usage': 'hdfs.lsr <args>',
    'quests': None,
}, {
    'usage': 'hdfs.mkdir [-p] <paths>',
    'quests': ['hdfs.mkdir /user/hadoop/dir1 /user/hadoop/dir2',
               'hdfs.mkdir hdfs://nn1.example.com/user/hadoop/dir hdfs://nn2.example.com/user/hadoop/dir'],
}, {
    'usage': 'hdfs.moveFromLocal <localsrc> <dst>',
    'quests': None,
}, {
    'usage': 'hdfs.moveToLocal [-crc] <src> <dst>',
    'quests': None,
}, {
    'usage': 'hdfs.mv URI [URI ...] <dest>',
    'quests': ['hdfs.mv /user/hadoop/file1 /user/hadoop/file2',
               'hdfs.mv hdfs://nn.example.com/file1 hdfs://nn.example.com/file2 hdfs://nn.example.com/file3 hdfs://nn.example.com/dir1'],
}, {
    'usage': 'hdfs.put <localsrc> ... <dst>',
    'quests': ['hdfs.put localfile /user/hadoop/hadoopfile',
               'hdfs.put localfile1 localfile2 /user/hadoop/hadoopdir',
               'hdfs.put localfile hdfs://nn.example.com/hadoop/hadoopfile',
               'hdfs.put - hdfs://nn.example.com/hadoop/hadoopfile'],
}, {
    'usage': 'hdfs.rm [-skipTrash] URI [URI ...]',
    'quests': ['hdfs.rm hdfs://nn.example.com/file /user/hadoop/emptydir'],
}, {
    'usage': 'hdfs.rmr [-skipTrash] URI [URI ...]',
    'quests': ['hdfs.rmr /user/hadoop/dir',
               'hdfs.rmr hdfs://nn.example.com/user/hadoop/dir'],
}, {
    'usage': 'hdfs.setrep [-R] [-w] <numRepicas> <path>',
    'quests': ['hdfs.setrep -w 3 /user/hadoop/dir1'],
}, {
    'usage': 'hdfs.stat URI [URI ...]',
    'quests': ['hdfs.stat path'],
}, {
    'usage': 'hdfs.tail [-f] URI',
    'quests': ['hdfs.tail pathname'],
}, {
    'usage': 'hdfs.test -[ezd] URI',
    'quests': ['hdfs.test -e filename'],
}, {
    'usage': 'hdfs.text <src>',
    'quests': None,
}, {
    'usage': 'hdfs.touchz URI [URI ...]',
    'quests': ['hdfs.touchz pathname'],
}, {
    'usage': 'hdjar.aggregatewordcount <inputDirs> <outputDir>',
    'quests': ['hdjar.aggregatewordcount inputdir1 inputdir2 inputdir3 outputdir'],
}, {
    'usage': 'hdjar.aggregatewordhist <inputDirs> <outputDir>',
    'quests': ['hdjar.aggregatewordhist inputdir1 inputdir2 inputdir3 outputdir'],
}, {
    'usage': 'hdjar.grep <inputDir> <outputDir> <regex> [<group>]',
    'quests': ['hdjar.grep inputdir outputdir "regex"',
               'hdjar.grep inputdir outputdir "regex" group',
               'hdjar.grep inputdir outputdir "regex" "group"'],
}, {
    'usage': 'hdjar.multifilewc <inputDir> <outputDir>',
    'quests': ['hdjar.multifilewc inputdir outputdir'],
}, {
    'usage': 'hdjar.randomtextwriter [-outFormat <outputFormatClass>] <outputDir>',
    'quests': ['hdjar.randomtextwriter -outFormat output.format.class outputdir',
               'hdjar.randomtextwriter outputdir'],
}, {
    'usage': 'hdjar.randomwriter <outdir>',
    'quests': ['hdjar.randomwriter outputdir'],
}, {
    'usage': 'hdjar.secondarysort <inputDir> <outputDir>',
    'quests': ['hdjar.secondarysort inputdir outputdir'],
}, {
    'usage': 'hdjar.wordcount <inputDir> <outputDir>',
    'quests': ['hdjar.wordcount inputdir outputdir'],
}]

def __GetHdfsUris_ut():
    for case in __testcases:
        print '------------------------------------------------------------'
        print '**  %s  **' % case['usage']
        quests = case['quests']
        if not quests:
            continue
        for quest in quests:
            print '\t' + quest
            hdfsuris = GetHdfsUris('hdfs', 'sugon.com', 'home', 'gxd', quest)
            print "\t\t\033[32m[URIS] -- %s\033[0m" % str(hdfsuris)
            
if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1].lower() == '-d':
        Debug.NDEBUG = True
    __GetHdfsUris_ut()

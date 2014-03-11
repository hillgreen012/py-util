import re

__patternCache = {}

def __MNRegex(name, element, m, n, isGreedy):
    # assert name and name.strip()
    assert element and element.strip()
    assert m <= n or n == -1
    assert not (m == -1 and n == -1)

    ret = None

    if 0 <= m <= n:
        ret = '(?P<%s>%s{%d,%d})' % (name, element, m, n)
    elif 0 <= m and n == -1:
        ret = '(?P<%s>%s{%d,})' % (name, element, m)
    elif m == -1:
        ret = '(?P<%s>%s{%d})' % (name, element, n)

    assert not ret

    if not isGreedy:
        ret = ret[0:-1] + '?' + ret[-1]

    return ret
        
def LazyMNRegex(name, element, m, n):
    return __MNRegex(name, element, m, n, False)

def GreedyMNRegex(name, element, m, n):
    return __MNRegex(name, element, m, n, True)

def LazyCompile(pattern):
    """pattern -> re compiled object with re.VERBOSE and re.DOTALL flags"""
    global __patternCache
    if pattern not in __patternCache:
        __patternCache[pattern] = re.compile(pattern, re.X | re.S)
    return __patternCache[pattern]
    
def SpawnMatcher(pattern):
    return LazyCompile(pattern).match
    
def SpawnSearcher(pattern):
    return LazyCompile(pattern).search

def Search(pattern, text):
    searched = SpawnSearcher(pattern)(text)
    return searched

def Match(pattern, text):
    matched = SpawnMatcher(pattern)(text)
    return matched

def HasSearched(pattern, text):
    return Search(pattern, text) is not None

def HasMatched(pattern, text):
    return Match(pattern, text) is not None

def WrapLeftRegex(regex):
    return r'^\s*%s' % regex

def WrapRightRegex(regex):
    return r'%s\s*$' % regex

def WrapArroundRegex(regex):
    return r'^\s*%s\s*$' % regex

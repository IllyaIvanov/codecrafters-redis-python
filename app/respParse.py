
import re

# v/\(RPUSH\|PING\|GET\|SET\|LRANGE\|ECHO\|LPUSH\|LPOP\|LLEN\|TYPE\|XADD\|XRANGE\|XREAD\|MULTI\|EXEC\|INCR\|DISCARD\|WATCH\|REPLCONF\|INFO\|PSYNC\|FULLRESYNC\)/visual

prefline = '-:$'
command_list =['RPUSH', 'PING', 'GET', 'SET', 'LRANGE', 'ECHO', 'LPUSH',
               'LPOP', 'LLEN', 'TYPE', 'XADD', 'XRANGE', 'XREAD', 'MULTI',
               'EXEC', 'INCR', 'DISCARD', 'WATCH', 'REPLCONF', 'INFO', 'PSYNC',
               'FULLRESYNC', 'REPLCONF']

def parse_rdb(rdb_string):
    return(["RDB"])

def parse_element(groups, elements):
    print('\n')
    
    print('Before parsing,')
    print(f'groups is {groups}')
    print(f'and elements is {elements}')
    if (len(elements) == 0
        or len(elements) == 1 and not elements[0]):
            print("parse_element error: elements is empty")
            return(groups, None)
    current = elements[0]
    print(f'current is {current}')
    #print(f'current is {current}')
    del elements[0]
    if current == b'$88' and elements[0][:5] == b'REDIS':
        groups.append(parse_rdb(current + elements[0]))
        del elements[0]
        return(groups, elements)
    indicator = chr(current[0])
    #print(f'indicator is {indicator}')
    if current == b'*1':
        return(groups, elements)
    elif indicator == '+':
        if current[0:11] == b'+FULLRESYNC':
            groups.append(['FULLRESYNC', current[12:].decode("utf-8")])
        else:
            groups.append(current.decode("utf-8")[1:])
    elif indicator == '$':
        print('bulk string, elements are ',elements)
        if (len(elements) >= 3
            and  elements[0] == b'REPLCONF' 
            and elements[2] == b'GETACK'):
                print(f'groups is {groups}')
                print('appending replconf getack whatever')
                groups.append(['REPLCONF', 'GETACK', elements[4]])
                print(f'BUT! now groups is {groups}')
                elements = elements[5:] 
                print(f'and, after parsing, now elements are {elements}')
        else:
            print('just a bulk string, nothing to see here')
            groups.append(elements[0].decode("utf-8"))
            del elements[0]
    elif indicator in prefline:
        #print('normal prefix')
        groups.append(elements[0].decode("utf-8"))
        del elements[0]
    elif indicator == '*':
        if current == b'*':
            return(groups, elements)
        #print('array prefix')
        groups.append([])
        array_length = int(current[1:].decode("utf-8"))
        for i in range(array_length): 
            parse_element(groups[-1], elements)
    else:
        #print('no prefix')
        try:
            groups[-1].append(current.decode("unicode-escape"))
        except:
            groups.append(current.decode("unicode-escape"))

    print('After parsing one step,')
    print("now groups are", groups)
    print("and elements are", elements)
    print('\n') 
    return(groups, elements)

def decode_resp(inline):
    #print(f'decoding inline {inline}')
    res = []
    moreparts = inline.split(b'\r\n') 
    print(f'moreparts is {moreparts}')

    while moreparts:
        resandparts = parse_element(res, moreparts)
        print(f'\nresandparts is {resandparts}')
        res, moreparts = resandparts
        print(f'res is {res}')
        print(f'moreparts is {moreparts}\n')
    
    # old decoding returned a list, but not a list of a single list?
    while len(res) == 1 and isinstance(res[0], list):
        #print('extracting res', res)
        res = res[0]
    print('decoded',res)
    return(res)

### command patterns ###
def old_decode(inline):
    #return alt_decode(inline)
    #print(f'normal decoding inline {inline}')
    prefix = chr(inline[0]) #chr -- converts single byte char to actual char
    res = None
    if prefix == '+': #simple string
        #print('simple string start')
        dangerous = inline[0:11] ### if it's fullresync --
        # do nothing
        #print(f'dangerous is {dangerous}')
        if dangerous == b'+FULLRESYNC':
            return('haha! fullresync')
        #todo parse rdb files
        #todo parse multiple commands
        #print('got a simple string')
        return inline.decode("utf-8")[1:-2]
        #return inline.decode("utf-8")[1:-4]
        #have I even been decoding simple strings at any point?
    elif prefix =='-': #error
        return inline.decode("utf-8")[1:-4]
    elif prefix ==':': #int
        if inline[1] == '+':
            return int(inline.decode("utf-8")[2:-4])
        else:
            return int(inline.decode("utf-8")[1:-4])
    elif prefix =='$': #bulk string
        return('haha! bulk string')
        inStr = inline.split(b'\r\n')[1]
        if inStr == b'':
            return ''
        else:
            return inStr[1].decode("utf-8")
    elif prefix =='*': #array
        res = []
        lines = inline.split(b'\r\n')
        #print('lines are', lines)
        count = int(lines[0][1:])
        for i in range(count):
            res.append(lines[2*i+2].decode("utf-8"))
        return res

def enSimple(toSend):
    return b'+' + toSend.encode("utf-8") + b'\r\n'

def enErr(toSend):
    return b'-' + toSend.encode("utf-8") + b'\r\n'

def encode_out(result):
    #print(result)
    if result is None:
        return b''
    toSend = result[0]
    outType = result[1]

    #print('toSend is ', toSend)
    body = b''
    header = tail = b'\r\n'
    body = str(toSend).encode("utf-8")
    if outType == 'unknown':
        if isinstance(toSend, int):
            outType = 'integer'
        elif isinstance(toSend, list):
            outType = 'array'
        elif isinstance(toSend, str):
            outType = 'bulk_string'

    #print('outType is', outType)
    match outType:
        case 'result_list':
            if not toSend:
                return b'*0\r\n'
            tail = b''
            body = b''
            header = b'*' + str(len(toSend)).encode("utf-8") + header
            while toSend:
                body += encode_out(toSend[0])
                toSend = toSend[1:]
        case 'integer':
            #print('encoding integer', toSend)
            header = b':'
            body = str(toSend).encode("utf-8")
        case 'bulk_string':
            #print('encoding bulk string', toSend)
            header = b'$' + str(len(toSend)).encode("utf-8") + header
        case 'array':
            body = b''
            if not toSend:
                return b'*0\r\n'
            if isinstance(toSend, str):
                toSend = toSend.split(' ')
            #print('encoding array', toSend)
            header = b'*' + str(len(toSend)).encode("utf-8") + header
            tail = b''
            #print(f'respParse: array {toSend} length is {len(toSend)}')
            for i in toSend:
                body += encode_out((i,'unknown'))
        case 'file':
            toSend = str(toSend)
            header = b'$' + str(len(toSend)).encode("utf-8") + b'\r\n'
            body = toSend.encode("utf-8")
            tail = b''
        case 'rdb':
            tail = b''
            header = f'${len(toSend)}\r\n'.encode("utf-8")
            body = toSend
        case 'result_sequence': #when we need to return multiple messages
            ans = []
            for i in toSend:
                ans.append(encode_out(i))
            return ans
        case 'simple_string':
            header = b'+'
        case 'simple_error':
            header = b'-'
        case 'null_array':
            return b'*-1\r\n'
        case 'null_bulk_string':
            return b'$-1\r\n'


    #print(f'header {header}, body {body}, tail {tail}')
    return header + body + tail

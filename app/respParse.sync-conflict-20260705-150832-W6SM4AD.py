#wew lad I'll need to split the string by commands??? dang
import re

prefix_dict = {
        '+' : 'simple string',
        '-' : 'error',
        ':' : 'integer',
        '$' : 'bulk string OR an rdb file',
        '*' : 'array'
        }

prefices_string = "".join(prefix_dict)
for i in prefices_string:
    print(f'{i} is a prefix')
prefix_pattern =f'([{prefices_string}]\\d+\\\\r\\\\n|[+-])'
print(f'prefix_pattern is {prefix_pattern}')
prefix_regexp = re.compile(prefix_pattern)
print(f'prefix_regexp is {prefix_regexp}')

def decode_resp(byteline):
    print(f'byteline is {byteline}')
    try:
        inline = byteline.decode("utf-8")
        inline=inline.encode('unicode_escape').decode()
    except:
        print('couldn\'t decode')
        return
    res = []
    line_list = split_resp(inline)
    print(f'line_list is {line_list}')
    for line in line_list:
        print(f'line is {line}')
        decoded_line = decode_single(line)
        print(f'decoded {line} is {decoded_line}')
        res.append(decoded_line)
        print(f'res is {res}')
    return(res)

def split_resp(inline):
    print(f'inline is {inline}')
    print(f'delimiters are {prefix_regexp.findall(inline)}')
    parts_list = re.split(prefix_regexp, inline)[1:]
    print(f'parts_list is {parts_list}')
    print(f'len(parts_list) is {len(parts_list)}')
    if len(parts_list) % 2 == 1:
        parts_list.append('')
    line_list = []
    for i in range(0, len(parts_list), 2):
        line_list.append(parts_list[i] + parts_list[i+1])
    return(line_list)

def decode_single(inline):
    print(f'decoding single inline {inline}')
    #prefix = chr(inline[0]) #chr -- converts single byte char to actual char
    # prev line -- from when it still was a byte
    prefix = inline[0]
    res = None
    print(f'prefix is {prefix}, got {prefix_dict.get(prefix)}')
    if prefix == '+': #simple string
        #print('got a simple string', inline )
        intro = inline[0:11]
        print(f'intro is {intro}')
        if intro == b'+FULLRESYNC':
            print(r'wow that\'s bad I\'ll process fullresync later')
            return ['fullresync'] 
        else:
            return inline[1:-2] #error: there's rdb, so can't decode?
            #return inline.decode("utf-8")[1:-2] #error: there's rdb, so can't decode?
        #return inline.decode("utf-8")[1:-4]
        return inline[1:-4]
        #have I even been decoding simple strings at any point?
    elif prefix =='-': #error
        return inline[1:-4]
        #return inline.decode("utf-8")[1:-4]
    elif prefix ==':': #int
        if inline[1] == '+':
            #return int(inline.decode("utf-8")[2:-4])
            return int(inline[2:-4])
        else:
            #return int(inline.decode("utf-8")[1:-4])
            return int(inline[1:-4])
    elif prefix =='$': #bulk string
        inStr = inline.split(b'\r\n')[1]
        print(f'inStr is {inStr}')
        #print('got a bulk string', inStr)
        if inStr == b'':
            return ''
        else:
            return inStr[1]
            #return inStr[1].decode("utf-8")
    elif prefix =='*': #array
        res = []
        lines = inline.split(b'\r\n')
        #print(lines)
        count = int(lines[0][1:])
        for i in range(count):
            res.append(lines[2*i+2])
            #res.append(lines[2*i+2].decode("utf-8"))
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

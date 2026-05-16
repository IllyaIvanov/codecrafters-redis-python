
def decode_resp(inline):
    prefix = chr(inline[0]) #chr -- converts single byte char to actual char
    res = None
    if prefix == '+': #simple string
        return inline.decode("utf-8")[1:-4]
    elif prefix =='-': #error
        return inline.decode("utf-8")[1:-4]
    elif prefix ==':': #int
        if inline[1] == '+':
            return int(inline.decode("utf-8")[2:-4])
        else:
            return int(inline.decode("utf-8")[1:-4])
    elif prefix =='$': #bulk string
        inStr = inline.split(b'\r\n')[1]
        if inStr == b'':
            return ''
        else:
            return inStr[1].decode("utf-8")
    elif prefix =='*': #array
        res = []
        lines = inline.split(b'\r\n')
        #print(lines)
        count = int(lines[0][1:])
        for i in range(count):
            res.append(lines[2*i+2].decode("utf-8"))
        return res

def enSimple(toSend):
    return b'+' + toSend.encode("utf-8") + b'\r\n'

def enErr(toSend):
    return b'-' + toSend.encode("utf-8") + b'\r\n'

def encode_out(result):
    print(result)
    toSend = result[0]
    outType = result[1]

    print('toSend is ', toSend)
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

    print('outType is', outType)
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
        # print('encoding list', toSend)
            if not toSend:
                return b'*0\r\n'
            header = b'*' + str(len(toSend)).encode("utf-8") + header
            tail = b''
            for i in toSend:
                body += encode_out((i,'unknown'))
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


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

def encode_out(toSend):
    body = b''
    header = tail = b'\r\n'
    if isinstance(toSend, int): 
        #print('encoding integer', toSend)
        header = b':' + header
        body = str(toSend).encode("utf-8") 
    elif isinstance(toSend, str): #bulk string
        #print('encoding bulk string', toSend)
        header = b'$' + str(len(toSend)).encode("utf-8") + header
        body = toSend.encode("utf-8")
    elif isinstance(toSend, list):
        print('encoding list', toSend)
        if not toSend:
            return b'*0\r\n'
        header = b'*' + str(len(toSend)).encode("utf-8") + header 
        tail = b''
        for i in toSend: 
            # wait, what does it do if the thing is not array of strings? 
            # well, it better be array of strings. todo: make work for 
            # different list element types 
            #print('encoding', i)
            newstr = encode_out(i)
            #print('newstr is', newstr)
            body +=  newstr 
            #print('body is', body, '\n')
            #body += b'$' + str(len(i)).encode("utf-8") + b'\r\n' + \ str(i).encode("utf-8") + b'\r\n'


    #print(f'header {header}, body {body}, tail {tail}')
    return header + body + tail

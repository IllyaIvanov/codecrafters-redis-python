import socket  # noqa: F401
import threading
from datetime import datetime, timedelta


def main():
    def respIn(inline):
        prefix = chr(inline[0]) #chr -- converts single byte char to actual char
        res = None
        if prefix == '+': #simple string
            return
        elif prefix =='-': #error
            return
        elif prefix ==':': #int
            return
        elif prefix =='$': #bulk string
            return
        elif prefix =='*': #array
            res = []
            lines = inline.split(b'\r\n')
            #print(lines)
            count = int(lines[0][1:])
            for i in range(count):
                res.append(lines[2*i+2].decode("utf-8"))
        return res

    def respond(conn):
        outline = None
        varDict = {}
        exps = {}
        while True:
            data = conn.recv(1024)
            if data:
                timeIn = datetime.now()
                #print(f'data is {data}')
                inline = respIn(data)
                print('inline is', inline)
                if type(inline) == list:
                    cmd = inline[0].lower()
                    print('command is', cmd)

                    if cmd == 'echo':
                        outline = b'$' 
                        #print('command is echo, outline starts with', outline)

                        for i in inline[1:]:
                            print('i is', i)
                            curStr = str(i).encode("utf-8")
                            outline += str(len(curStr)).encode("utf-8")
                            outline += b'\r\n'
                            outline += str(i).encode("utf-8") 
                            outline += b'\r\n'
                    elif cmd == 'ping':
                        #print('command is ping')
                        outline = b'+PONG\r\n'
                    elif cmd == 'set':
                        outline =b'+OK\r\n' 
                        vName = inline[1]
                        vVal = inline[2]

                        if inline[3] and inline[4]: # optional expiry parameters
                            oName = inline[3]
                            oVal = int(inline[4])
                            print('oName, oVal', oName, oVal)

                            if oName == 'px':
                                exps[vName] = datetime.now() + timedelta(microseconds = (oVal * 1000))
                                print('will expire at', exps[vName])
                            elif oName == 'ex':
                                exps[vName] = datetime.now() + timedelta(seconds = oVal)
                                print('will expire at', exps[vName])
                        

                        varDict[vName] = vVal
                        print(f'set {vName} to {varDict.get(vName)}')
                        print('varDict is', varDict)
                    elif cmd == 'get':
                        print('varDict is', varDict)
                        vName = inline[1]
                        print('vName is', vName)
                        vVal = varDict.get(vName)
                        print('vVal is', vVal)
                        if vVal != None:
                            if not exps.get(vName) or (exps.get(vName) and exps[vName] < datetime.now()):
                                vOut = str(vVal)
                                l = str(len(vOut))
                                outline = b'$' + l.encode("utf-8") + b'\r\n' + vOut.encode("utf-8") + b'\r\n'
                            else:
                                outline = b'$-1\r\n'

                        else: 
                            outline = b'$-1\r\n'
                else:
                    outline = data
                print(outline) 
                conn.send(outline)
    
            #conn.sendall(b"+PONG\r\n") #key part --- there must be a loop in this function

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept() # _ is used because .accept() returns two values
        if not connection: 
            break
        thr = threading.Thread(target=respond, args=(connection,))
        thr.start()
        
        #for curcon in connections:
        #    data = curcon.recv(1024)
        #    if data:
        #        curcon.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()

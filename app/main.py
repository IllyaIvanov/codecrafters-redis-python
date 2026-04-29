# do a separate resp parser
# what is (match.string)?
# how to do proper case switch, and not 10000 elifs?
# importing files doesn't work

import socket  # noqa: F401
import threading
from datetime import datetime, timedelta

import app.respParse

def main():
    def respond(conn):
        outline = None
        varDict = {}
        exps = {}
        while True:
            data = conn.recv(1024)
            if data:
                timeIn = datetime.now()
                #print(f'data is {data}')
                inline = app.respParse.decode_resp(data)
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
                        if len(inline) > 3:
                            if inline[3] and inline[4]: 
                                # optional expiry parameters
                                oName = inline[3].lower()
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
                            if  exps.get(vName) and exps.get(vName) < datetime.now():
                                print(f'key {vName} expired')
                                outline = b'$-1\r\n'
                            else:
                                vOut = str(vVal)
                                l = str(len(vOut))
                                outline = b'$' + l.encode("utf-8") + b'\r\n' + vOut.encode("utf-8") + b'\r\n'
                        else: 
                            outline = b'$-1\r\n'

                    elif cmd == 'rpush':
                        listName = inline[1] #making the list we add
                        if len(inline) == 3:
                            listExtra = [inline[2]]
                        else:
                            listExtra = inline[2:]
                        if varDict.get(listName) != None:
                            varDict[listName] += listExtra #adding the new part to the existing list
                        else:
                            varDict[listName] = listExtra #if no list, make it
                        l = len(varDict[listName])
                        outline = b':' + str(l).encode("utf-8") + b'\r\n'

                    elif cmd == 'lpush':
                        listName = inline[1] #making the list we add
                        if len(inline) == 3:
                            listExtra = [inline[2]]
                        else:
                            listExtra = inline[:1:-1]
                        if varDict.get(listName) != None:
                            varDict[listName] = listExtra + varDict[listName] #adding the new part to the existing list
                        else:
                            varDict[listName] = listExtra #if no list, make it
                        l = len(varDict[listName])
                        outline = b':' + str(l).encode("utf-8") + b'\r\n'
 

                    elif cmd == 'lrange':
                        listName = inline[1]
                        if varDict.get(listName) == None:
                            outline = b'*0\r\n'
                        else:
                            tList = varDict.get(listName)
                            print('called list is', tList)
                            n = len(tList)
                            print('its length is', n)
                            inds = [int(j) for j in inline[2:4]]
                            for i in range(2):
                                if inds[i] + n < 0:
                                    inds[i] = 0
                                elif inds[i] < 0:
                                    inds[i] = n + inds[i]
                            print('inds are', inds)
                            tList = tList[inds[0]:inds[1]+1]
                            outline = app.respParse.encode_out(tList)
                    
                    elif cmd == 'llen':
                        listName = inline[1]
                        tList = varDict.get(listName)
                        if tList == None:
                            outline = b':0\r\n'
                        else:
                            l = len(tList)
                            outline = b':' + str(l).encode("utf-8") + b'\r\n'

                    elif cmd == 'lpop':
                        listName = inline[1]
                        if varDict.get(listName) == None:
                            return b'-1\r\n'
                        k = 1
                        if inline[2]:
                            k = inline[2]
                        if len(varDict.get(listName)) <= k:
                            outline = app.respParse.encode_out(varDict[listName])
                            varDict[listName] = []
                        else:
                            outline = app.respParse.encode_out(varDict[listName][:k])
                            varDict[listName] = varDict[listName][k:]
                        
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
    #todo why the underscores? what does this specific initialization do?
    main()

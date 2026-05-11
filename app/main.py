# do a separate resp parser
# what is (match.string)?
# how to do proper case switch, and not 10000 elifs?
# importing files doesn't work

import socket  # noqa: F401
import threading
from datetime import datetime, timedelta
import time

import app.respParse


def main():
    def respond(conn):
        outline = None
        waitstarts = []
        varDict = {}
        exps = {}
        while True:
            data = conn.recv(1024)
            if data:
                timeIn = datetime.now()
                # print(f'data is {data}')
                inline = app.respParse.decode_resp(data)
                print('inline is', inline)
                if type(inline) == list:
                    cmd = inline[0].lower()
                    print('command is', cmd)

                    if cmd == 'echo':
                        outline = b'$'
                        for i in inline[1:]:
                            print('i is', i)
                            curStr = str(i).encode("utf-8")
                            outline += str(len(curStr)).encode("utf-8")
                            outline += b'\r\n'
                            outline += str(i).encode("utf-8")
                            outline += b'\r\n'

                    elif cmd == 'ping':
                        outline = b'+PONG\r\n'

                    elif cmd == 'set':
                        outline = b'+OK\r\n'
                        vName = inline[1]
                        vVal = inline[2]
                        if len(inline) > 3:
                            if inline[3] and inline[4]:
                                # optional expiry parameters
                                oName = inline[3].lower()
                                oVal = int(inline[4])
                                print('oName, oVal', oName, oVal)
                                if oName == 'px':
                                    exps[vName] = datetime.now() + \
                                        timedelta(microseconds=(oVal * 1000))
                                    print('will expire at', exps[vName])
                                elif oName == 'ex':
                                    exps[vName] = datetime.now() + \
                                        timedelta(seconds=oVal)
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
                            if exps.get(vName) and exps.get(vName) < datetime.now():
                                print(f'key {vName} expired')
                                outline = b'$-1\r\n'
                            else:
                                vOut = str(vVal)
                                l = str(len(vOut))
                                outline = b'$' + \
                                    l.encode("utf-8") + b'\r\n' + \
                                    vOut.encode("utf-8") + b'\r\n'
                        else:
                            outline = b'$-1\r\n'

                    elif cmd == 'rpush':
                        listName = inline[1]  # making the list we add
                        if len(inline) == 3:
                            listExtra = [inline[2]]
                        else:
                            listExtra = inline[2:]
                        if varDict.get(listName) != None:
                            # adding the new part to the existing list
                            varDict[listName] += listExtra
                        else:
                            # if no list, make it
                            varDict[listName] = listExtra
                        l = len(varDict[listName])
                        outline = b':' + str(l).encode("utf-8") + b'\r\n'
                        print(f'rpushed: {listName} is now {varDict.get(listName)}')

                    elif cmd == 'lpush':
                        listName = inline[1]  # making the list we add
                        if len(inline) == 3:
                            listExtra = [inline[2]]
                        else:
                            listExtra = inline[:1:-1]
                        if varDict.get(listName) != None:
                            # adding the new part to the existing list
                            varDict[listName] = listExtra + varDict[listName]
                        else:
                            # if no list, make it
                            varDict[listName] = listExtra
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
                        if len(inline) <= 2:
                            outline = app.respParse.encode_out(
                                varDict[listName][0])
                            varDict[listName] = varDict[listName][1:]
                        else:
                            k = int(inline[2])
                            if len(varDict.get(listName)) <= k:
                                outline = app.respParse.encode_out(
                                    varDict[listName])
                                varDict[listName] = []
                            else:
                                outline = app.respParse.encode_out(
                                    varDict[listName][:k])
                                varDict[listName] = varDict[listName][k:]

                    elif cmd == 'blpop':  # todo: make commands into functions of string,
                        # read the listname and timeout

                        # todo just compile it together? if received a list, then listName is ...
                        listName = inline[1]
                        timeOut = int(inline[2])
                        print('timeout is', timeOut)

                        # calculate when will the timeout expire

                        tExp = time.time() + timeOut
                        if waitstarts != []:
                            waitcount = waitstarts[-1]+1
                        else:
                            waitcount = 0
                        waitstarts.append(waitcount)
                        print('waitstarts is now', waitstarts)

                        a = True
                        chP = time.time()
                        print(f'waitcount: {waitcount}: first checkpoint is {chP}')

                        c = 0
                        while a: 
                            c += 1
                                
                            if timeOut != 0 and tExp < time.time():
                                print(f'waitcount {waitcount}: expired')
                                a = 'expired'
                                waitstarts.remove(waitcount)
                                outline = b'*-1\r\n'
                                break
                                
                            a = (waitstarts[-1] != waitcount) or (
                                not varDict.get(listName)) 

                            if time.time() - chP > 0.4:
                                c = 0
                                print(f'waitcount {waitcount}: prev chP {chP}, next checkpoint {time.time()} ')
                                chP = time.time()
                                if not varDict.get(listName):
                                    print(f'waitcount {waitcount}: list {listName} still empty')
                                    print(f'look at it: {varDict.get(listName)}')
                                elif  (waitstarts[-1] != waitcount):
                                    print(
                                        f'Not my turn: my waitcount is {waitcount}'
                                        f'but waitstarts are {waitstarts}'
                                          )
                                else:
                                    print(f'waitcount {waitcount}: all conditions are satisfied')
                        print(f'waitcount {waitcount}: done with the loop')

                        if a != 'expired':
                            outlist = [listName, varDict[listName][0]]
                            outline = app.respParse.encode_out(outlist)
                            varDict[listName] = varDict[listName][1:] 
                            waitstarts.remove(waitcount)

                            #delT = 10
                            #if timeOut != 0:
                            #    delT = timeOut*100
                            #popd = False
                            #chP = time.time()
                            #print('first checkpoint', chP)
                            #cT = 0
                            #while timeOut == 0:
                            #    # while timeOut == 0 or time.time() < tExp:
                            #    lst = varDict.get(listName)
                            #    tt = time.time()
                            #    if tt - chP > delT:
                            #        chp = tt
                            #        lst = varDict.get(listName)
                            #        print(f'next checkpoint {chp}, {cT}, lst')
                            #        cT += 1
                            #    if not lst in [None, []]:
                            #        print(
                            #            f'key {listName} is now {varDict.get(listName)} ')
                            #        popd = True
                            #        break
                            #if popd:
                            #    outline = app.respParse.encode_out([listName,
                            #                                        varDict[listName][0]])
                            #    varDict[listName] = varDict[listName][1:]
                            #else:
                            #    outline = b'*-1\r\n'

                else:
                    outline = data
                print(outline)
                conn.send(outline)

            # conn.sendall(b"+PONG\r\n") #key part --- there must be a loop in this function

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        # _ is used because .accept() returns two values
        connection, _ = server_socket.accept()
        if not connection:
            break

        thr = threading.Thread(target=respond, args=(connection,))
        if thr:
            print(thr)
        thr.start()

        # for curcon in connections:
        #    data = curcon.recv(1024)
        #    if data:
        #        curcon.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    # todo why the underscores? what does this specific initialization do?
    main()

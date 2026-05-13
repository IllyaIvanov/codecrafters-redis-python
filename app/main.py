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
    class stream:
        def __init__(self, id):
            self.id = id
            self.data = {}

    #now xadd can't read idMin from here
    waitstarts = []
    varDict = {} 
    # for some reason, blpop was not able to see the varDict when it was in 'respond', 
    #and all the other commands were able?? 
    #todo: read up on variable scope, look at others' implementations


    def respond(conn):
        idMin = [0,0] 
        #idMin doesn't work, if it's defined next to waitstarts, unlike varDict
        outline = None
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
                                    print('exps are', exps)
                                elif oName == 'ex':
                                    exps[vName] = datetime.now() + \
                                        timedelta(seconds=oVal)
                                    print('will expire at', exps[vName])
                                    print('exps are', exps)
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
                                print('exps are', exps)
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
                        print('rpushed varDict is', varDict)

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
                        ########### literally just copying the rpush
                        # todo just compile it together? if received a list, then listName is ...
                        listName = inline[1]
                        timeOut = float(inline[2])
                        print('timeout is', timeOut)
                        # calculate when will the timeout expire, also getting a number
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
                        while a: 
                            if timeOut != 0 and tExp < time.time():
                                print(f'waitcount {waitcount}: expired')
                                a = 'expired'
                                waitstarts.remove(waitcount)
                                outline = b'*-1\r\n'
                                break
                                
                            a = (waitstarts[0] != waitcount) or (
                                not varDict.get(listName)) 
                            if time.time() - chP > 0.4:
                                c = 0
                                print(f'waitcount {waitcount}: prev chP {chP}, next checkpoint {time.time()} ')
                                chP = time.time()
                                if not varDict.get(listName):
                                    print(f'waitcount {waitcount}: list {listName} still empty')
                                    print(f'look at it: {varDict.get(listName)}')
                                    print(f'and varDict is {varDict}')
                                elif  (waitstarts[0] != waitcount):
                                    print(
                                        f'Not my turn: my waitcount is {waitcount}, '
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

                    elif cmd == 'type':
                        listName = inline[1]
                        val = varDict.get(listName) 
                        if val == None:
                            outline = app.respParse.enSimple('none')
                        else:
                            res = str(type(val))
                            print('value type is', res)
                            res = res[8:-2]
                            print('res is', res)
                            match res:
                                case 'str':
                                    tip = 'string'
                                case 'list':
                                    tip = 'list'
                                case 'hash':
                                    tip = 'hash'
                                case 'set':
                                    tip = 'set'
                                case 'zset':
                                    tip = 'zset'
                                case '__main__.main.<locals>.stream':
                                    tip = 'stream'
                                case 'vectorset':
                                    tip = 'vectorset'
                                case _:
                                    #nes = '__main__.main.<locals>.stream'
                                    #if nes == res:
                                    #    print('they are literally the same value')
                                    #else:
                                    #    comp = ''
                                    #    print(f'lengths are {len(res)} and {len(nes)}')
                                    #    for i in range(len(res)):
                                    #        if res[i] == nes [i]:
                                    #            comp += res[i]
                                    #        else:
                                    #            comp += f'({res[i],nes[i]})'
                                    #print('comp is', comp)
                                    tip = 'unknown'
                            outline = app.respParse.enSimple(tip)

                    elif cmd == 'xadd':
                        print('idMin is', idMin)
                        errMsg = ''
                        # validate ID's
                        #seq.number-time in ms
                        # seq. number >=, time >
                        #first let's validate:
                        #invalid options: either val[0] is strictly less then min[0]
                        # idVal[0] != '*' and idVal[0] < idMin[0]
                        # or they're equal and val[1] <= val[0]

                        stream_is_new = False
                        streamKey = inline[1]
                        streamID = inline[2]
                        idVal = [stri for stri in streamID.split('-')]
                        #let's generate first:
                        if idVal == ['*']:
                            stream_is_new = True
                            idVal[0] = (1000 * time.time()) // 1
                            idVal[1] = '*'
                        if idVal[1] == '*':
                            stream_is_new = True
                            if idVal[0] == idMin[0]:
                                idVal[1] = idMin[1] + 1
                            else:
                                idVal[1] = 0
                        #now, let's validate:

                        if max(idVal) <= 0:
                                errMsg = 'ERR The ID specified in XADD must be greater than 0-0'
                        elif idVal[0] < idMin[0] or (idVal[0] == idMin[0] and idVal[1] <= idMin[1]):
                            errMsg = ('ERR The ID specified in XADD is equal or smaller than' + \ 
                                      ' the target stream top item')
                        if errMsg:
                            outline = app.respParse.enErr(errMsg)
                        else:
                            if stream_is_new:
                                idMin = idVal
                                print('stream is new')
                                streamID = '-'.join([str(x) for x in idVal])
                                varDict[streamKey] = stream(streamID)
                            else:
                                print('stream isn\'t new')
                            for i in range(3, len(inline), 2):
                                varDict[streamKey].data[str(inline[i])] = inline[i+1]
                            outline = app.respParse.encode_out(streamID)

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

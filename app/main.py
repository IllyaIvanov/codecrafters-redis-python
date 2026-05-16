# solved do a separate resp parser
# solved  how to do proper case switch, and not 10000 elifs? --- match x case 10 case 20...
# solved: importing files doesn't work --- codecrafters needs "app.respParse", not just "respParse"

import socket  # noqa: F401
import threading
from datetime import datetime, timedelta
import time

import app.respParse


def main():

    class stream:
        def __init__(self):
            self.ids = []
            self.data = {}
            self.idMin = [0, 0]

    waitstarts = []
    varDict = {}
    # for some reason, blpop was not able to see the varDict when it was in 'respond',
    # and all the other commands were able??
    # todo: read up on variable scope, look at others' implementations

    def idComp(id1, id2):  # comparing two stream id's
        # print(f'comparing ids {id1} and {id2}')
        v1 = [int(x) for x in id1.split('-')]
        v2 = [int(x) for x in id2.split('-')]
        c = 10*(v1[0] > v2[0]) - 10*(v1[0] < v2[0]) + \
            1 * (v1[1] > v2[1]) - 1 * (v1[1] < v2[1])
        # print(f'c is {c}')
        if c > 0:
            ans = '>'
        elif c == 0:
            ans = '='
        else:
            ans = '<'
        # print(id1, ans, id2)
        return ans

    def strmOut(streamKey, idlist):
        if not isinstance(idlist, list):
            idlist = [idlist]
        #print('idlist is', idlist)
        ans = []
        strm = varDict[streamKey]
        for i in idlist:
            ans.append([])
            # ans --- the total stream output
            # ans[i] ---id and then data under the this id
            ans[-1] = [i, []]
            #print('filling', ans[-1])
            # j is a key for this id's data
            for j in strm.data[i]:
                # print('strmOut is appending', j)
                ans[-1][1].append(j)
                # print('strmOut is appending', strm.data[i][j])
                ans[-1][1].append(strm.data[i][j])
                # ans[i][1].append(varDict[streamKey].data[j])
        return ans

        # todo: error messages

    def strmGet(streamKey, idB, idE, timeExp=False, excl=False):
        strm = varDict.get(streamKey)
        print(f'getting the range {idB} --- {idE} from stream{strm}')

        if strm == None:
            # print('stream not found somehow?')
            outline = app.respParse.enErr('Error: no such stream')
        else:
            #print('timeExp is', timeExp)
            i = inB = 0
            strB = ['>']
            if excl:
                strB.append('=')
            if timeExp != False:
                #print('there\'s timeExp', timeExp)
                chP = time.time()
                # print('first chP is', chP)
                intr = 0
                while (not strm.ids or idComp(idB, strm.ids[-1]) in strB) and (not isinstance(timeExp, float) or time.time() < timeExp):
                    #if intr == 0:
                        #print('we start whiling with ids', strm.ids)
                        #print('timeExp is ', timeExp)
                        #print('we need to surpass', idB)
                    if intr >= 1000000:
                        intr = 1
                        #print(f'strm.ids are {strm.ids}, ')
                    intr += 1

                print('loop exited')
                if isinstance(timeExp, float) and time.time() > timeExp and idComp(idB, strm.ids[-1]) in strB:
                    print('time out')
                    return 'nil'
            if idB != '-':
                while idComp(idB, strm.ids[i]) in strB and i < len(strm.ids):
                    i += 1
                inB = i
            inE = len(strm.ids)-1
            if idE != '+':
                while idComp(idE, strm.ids[i]) == '>':
                    i += 1
                inE = i
            # if i == len(strm.ids) - 1:
            #    idE = -1
            # print(f'idB and idE are {idB,idE}')
            # print(f'respective ids are{strm.ids[idB], strm.ids[idE]}')
            # print(f'so idlist is {strm.ids[idB:idE+1]}')
            res = strmOut(streamKey, strm.ids[inB:inE+1])
            return res

    def respond(conn):
        outline = None
        exps = {}
        while True:
            data = conn.recv(1024)
            if data:
                timeIn = datetime.now()
                inline = app.respParse.decode_resp(data)
                print('inline is', inline)
                if type(inline) == list:
                    cmd = inline[0].lower()
                    print('command is', cmd)

                    if cmd == 'echo':
                        outline = b'$'
                        for i in inline[1:]:
                            # print('i is', i)
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
                                # print('oName, oVal', oName, oVal)
                                if oName == 'px':
                                    exps[vName] = datetime.now() + \
                                        timedelta(microseconds=(oVal * 1000))
                                    # print('will expire at', exps[vName])
                                    # print('exps are', exps)
                                elif oName == 'ex':
                                    exps[vName] = datetime.now() + \
                                        timedelta(seconds=oVal)
                                    # print('will expire at', exps[vName])
                                    # print('exps are', exps)
                        varDict[vName] = vVal
                        # print(f'set {vName} to {varDict.get(vName)}')
                        # print('varDict is', varDict)

                    elif cmd == 'get':
                        # print('varDict is', varDict)
                        vName = inline[1]
                        # print('vName is', vName)
                        vVal = varDict.get(vName)
                        # print('vVal is', vVal)
                        if vVal != None:
                            if exps.get(vName) and exps.get(vName) < datetime.now():
                                # print(f'key {vName} expired')
                                outline = b'$-1\r\n'
                                # print('exps are', exps)
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
                        # print(f'rpushed: {listName} is now {varDict.get(listName)}')
                        # print('rpushed varDict is', varDict)

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
                            # print('called list is', tList)
                            n = len(tList)
                            # print('its length is', n)
                            inds = [int(j) for j in inline[2:4]]
                            for i in range(2):
                                if inds[i] + n < 0:
                                    inds[i] = 0
                                elif inds[i] < 0:
                                    inds[i] = n + inds[i]
                            # print('inds are', inds)
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
                        # literally just copying the rpush
                        # todo just compile it together? if received a list, then listName is ...
                        listName = inline[1]
                        timeOut = float(inline[2])
                        # print('timeout is', timeOut)
                        # calculate when will the timeout expire, also getting a number
                        tExp = time.time() + timeOut
                        if waitstarts != []:
                            waitcount = waitstarts[-1]+1
                        else:
                            waitcount = 0
                        waitstarts.append(waitcount)
                        # print('waitstarts is now', waitstarts)
                        a = True
                        chP = time.time()
                        # print(f'waitcount: {waitcount}: first checkpoint is {chP}')
                        while a:
                            if timeOut != 0 and tExp < time.time():
                                # print(f'waitcount {waitcount}: expired')
                                a = 'expired'
                                waitstarts.remove(waitcount)
                                outline = b'*-1\r\n'
                                break

                            a = (waitstarts[0] != waitcount) or (
                                not varDict.get(listName))
                            if time.time() - chP > 0.4:
                                c = 0
                                # print(f'waitcount {waitcount}: prev chP {chP}, next checkpoint {time.time()} ')
                                chP = time.time()
                                # if not varDict.get(listName):
                                # print(f'waitcount {waitcount}: list {listName} still empty')
                                # print(f'look at it: {varDict.get(listName)}')
                                # print(f'and varDict is {varDict}')
                                # elif  (waitstarts[0] != waitcount):
                                # print(
                                #    f'Not my turn: my waitcount is {waitcount}, '
                                #    f'but waitstarts are {waitstarts}'
                                #      )
                                # else:
                                # print(f'waitcount {waitcount}: all conditions are satisfied')
                        # print(f'waitcount {waitcount}: done with the loop')
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
                            # print('value type is', res)
                            res = res[8:-2]
                            # print('res is', res)
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
                                    # nes = '__main__.main.<locals>.stream'
                                    # if nes == res:
                                    #    print('they are literally the same value')
                                    # else:
                                    #    comp = ''
                                    #    print(f'lengths are {len(res)} and {len(nes)}')
                                    #    for i in range(len(res)):
                                    #        if res[i] == nes [i]:
                                    #            comp += res[i]
                                    #        else:
                                    #            comp += f'({res[i],nes[i]})'
                                    # print('comp is', comp)
                                    tip = 'unknown'
                            outline = app.respParse.enSimple(tip)

                    elif cmd == 'xadd':
                        errMsg = ''
                        streamKey = inline[1]
                        streamID = inline[2]
                        strm = varDict.get(streamKey)
                        if strm == None:
                            strm = stream()
                        idMin = strm.idMin
                        idVal = [stri for stri in streamID.split('-')]
                        # let's generate first:
                        if idVal == ['*']:
                            # print('generating [0]')
                            idVal[0] = int(1000 * time.time())
                            # print('idVal[0] is', idVal[0])
                            idVal.append('*')
                        else:
                            idVal[0] = int(idVal[0])
                        if idVal[1] == '*':
                            # print('generating[1]')
                            if idVal[0] == idMin[0]:
                                idVal[1] = idMin[1] + 1
                            else:
                                idVal[1] = 0
                            # print('idVal[1] is', idVal[1])
                        else:
                            idVal[1] = int(idVal[1])
                        # print('idVal is', idVal)
                        streamID = '-'.join([str(x) for x in idVal])
                        # now, let's validate:
                        if max(idVal) <= 0:
                            errMsg = 'ERR The ID specified in XADD must be greater than 0-0'
                        elif idVal[0] < idMin[0] or (idVal[0] == idMin[0] and idVal[1] <= idMin[1]):
                            errMsg = ('ERR The ID specified in XADD is equal or smaller than '
                                      'the target stream top item')
                        if errMsg:
                            outline = app.respParse.enErr(errMsg)
                        else:
                            if varDict.get(streamKey) == None:
                                # print('stream is new')
                                varDict[streamKey] = stream()
                            # else:
                                # print('stream isn\'t new')
                            varDict[streamKey].idMin = idVal
                            # todo get read of idMin, that's just the last element of ids
                            varDict[streamKey].ids.append(streamID)
                            varDict[streamKey].data[streamID] = {}
                            for i in range(3, len(inline), 2):
                                varDict[streamKey].data[streamID][str(
                                    inline[i])] = inline[i+1]
                            # print(f'stream \'{streamKey}\' ids are now {varDict[streamKey].ids}')
                            outline = app.respParse.encode_out(streamID)

                    elif cmd == 'xrange':
                        # print('starting xrange')
                        streamKey = inline[1]
                        rB = inline[2]
                        rE = inline[3]
                        strm = varDict.get(streamKey)
                        outline = app.respParse.encode_out(
                            strmGet(streamKey, rB, rE))

                    elif cmd == 'xread':
                        excl = True
                        # print('starting xrange')
                        if inline[1] == 'block':
                            timeOut = int(inline[2])
                            if timeOut == 0:
                                timeExp = True
                            else:
                                timeExp = time.time() + timeOut/1000
                            i = 4
                            print('timeOut is', timeOut, 'timeExp is', timeExp)
                        else:
                            timeExp = False
                            i = 2
                        keys = []
                        print(f'i is {i}')
                        while inline[i] in varDict:
                            #print('getting key', inline[i])
                            keys.append(inline[i])
                            i += 1
                        ids = inline[i:]
                        #print(f'keys are {keys}, ids are {ids}')
                        for j in range(len(ids)):
                            if ids[j] == '$':
                                ids[j] = varDict[keys[j]].ids[-1]
                        
                        res = []
                        while not keys and (timeExp == True or time.time() < timeExp):
                            res = []
                        for i in range(len(keys)):
                            #print(f'getting chunk {ids[i]} of {keys[i]}')
                            #if timeExp != False:
                                #print( f'or waiting {timeExp - time.time()} more')
                            chunk = strmGet(
                                keys[i], ids[i], '+', timeExp, excl)
                            if chunk == 'nil':
                                res = []
                                break
                            else:
                                res.append([keys[i], chunk])
                                #print('the chunk is', chunk)
                        #print('res is', res)
                        if res != []:
                            outline = app.respParse.encode_out(res)
                        else:
                            outline = b'*-1\r\n'
                            # todo --- encode empty list like that?

                       # print('starting xrange')

                else:
                    outline = data
                conn.send(outline)
                print('sent outline is', outline)

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

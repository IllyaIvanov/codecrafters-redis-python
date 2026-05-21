# solved do a separate resp parser
# solved  how to do proper case switch, and not 10000 elifs? --- match x case 10 case 20...
# solved: importing files doesn't work --- codecrafters needs "app.respParse", not just "respParse"

#varDict\[[^[]*]\s*=
#search for modified varDict things

#ok, so I... don't think the key can be modified, and then restored within a
#same command? so rough way is to compare vardict before with vardict after,
# and mark the changed keys
#or mark all the var-changing commands? tbh yeah, that might be

######################
'''
Isn't it time to just... define commands as a class? with tags and such,
"modifying", :
'''
import socket  # noqa: F401
import threading
from datetime import datetime, timedelta
import time

import app.respParse


def main():
    #consts
    dMod = ('incr', 'xadd', 'blpop', 'lpop', 'lpush', 'rpush', 'exec')

    exps = {}
    waitstarts = []
    responds=[] #client ids essentially
    varDict = {}
    qDicts = {} #{'charging' : False, 'cmdQ' : []}
    keyWatchTimes = {}
    keyModTimes = {}

    def defaultize(qdict):
        qdict['charging'] = False
        qdict['cmdQ'] = []
        qdict['keyQ'] = {}



    class stream:
        def __init__(self):
            self.ids = []
            self.data = {}
            self.idMin = [0, 0]

    def idComp(id1, id2):  # comparing two stream id's
        #print(f'comparing ids {id1} and {id2}')
        v1 = [int(x) for x in id1.split('-')]
        v2 = [int(x) for x in id2.split('-')]
        c = 10*(v1[0] > v2[0]) - 10*(v1[0] < v2[0]) + \
            1 * (v1[1] > v2[1]) - 1 * (v1[1] < v2[1])
        #print(f'c is {c}')
        if c > 0:
            ans = '>'
        elif c == 0:
            ans = '='
        else:
            ans = '<'
        #print(id1, ans, id2)
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
                #print('strmOut is appending', j)
                ans[-1][1].append(j)
                #print('strmOut is appending', strm.data[i][j])
                ans[-1][1].append(strm.data[i][j])
                # ans[i][1].append(varDict[streamKey].data[j])
        return ans

        # todo: error messages

    def strmGet(streamKey, idB, idE, timeExp=False, excl=False):
        strm = varDict.get(streamKey)
        #print(f'getting the range {idB} --- {idE} from stream{strm}')
        if strm == None:
            #print('stream not found somehow?')
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
                #print('first chP is', chP)
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
                #print('loop exited')
                if isinstance(timeExp, float) and time.time() > timeExp and idComp(idB, strm.ids[-1]) in strB:
                    #print('time out')
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
            #print(f'idB and idE are {idB,idE}')
            #print(f'respective ids are{strm.ids[idB], strm.ids[idE]}')
            #print(f'so idlist is {strm.ids[idB:idE+1]}')
            res = strmOut(streamKey, strm.ids[inB:inE+1])
            return res

    # for some reason, blpop was not able to see the varDict when it was in 'respond',
    # and all the other commands were able??
    # todo: read up on variable scope, look at others' implementations

    def qGet(feedee):
        res = []
        for i in qDicts[feedee]['cmdQ']:
            res.append(i[0])
        res = [elt[0] for elt in qDicts[feedee]['cmdQ']]
        return res


    def exCmd(inline, reNo): 
        print(f'reNo {reNo}: executing {inline[0].upper()}')
        #print('exCmd thinks that reNo is', reNo)
        if qDicts.get(reNo) == None:
            qDicts[reNo] = {}
            defaultize(qDicts[reNo])
        if keyWatchTimes.get(reNo) == None:
            keyWatchTimes[reNo] = {}

       #print('reNo', reNo, ':', 'qDict is', qDicts[reNo])
        #exCmd sees it, until I refer to it from respond. hm.
        if type(inline) == list:
            cmd = inline[0].lower()
        else:
            cmd = inline
       #print('reNo', reNo, ':', 'command is', cmd.upper())
       #print('reNo', reNo, ': inline is', inline)

       
        #solved: http://christophe.vandeplas.com/2011/06/python-global-variables.html
        #varDict is not assigned within the function, so it ?remains
        #global? // I have no idea, why varDict is accessible, but charging isn't

        if cmd == 'exec':
            thisWatch = keyWatchTimes.get(reNo)
            if thisWatch:
                keyWatchTimes[reNo] = {}
                for i in thisWatch: # i is [key, wTime]
                    print(f'reNo {reNo}: key {i} was watched at {thisWatch[i]}')
                    print(f'reNo {reNo}: key {i} was changed at {keyModTimes.get(i)}')
                    if thisWatch[i] < keyModTimes[i]:
                        defaultize(qDicts[reNo])
                        return ('', 'null_array')
            #print('reNo', reNo, ':', 'Casting:') #print('reNo', reNo, ': qDict is', qDicts.get(reNo))
            if not qDicts[reNo]['charging']:
                return('ERR EXEC without MULTI', 'simple_error')
                #outline = app.respParse.enErr('ERR EXEC without MULTI')
            qDicts[reNo]['charging'] = False
            if not qDicts[reNo]['cmdQ']:
                return([],'array')
                #outline = app.respParse.encode_out([])
            else:
                res = []
                while qDicts[reNo]['cmdQ']:
                   #print('reNo', reNo, ':', 'Casting', qDicts[reNo]['cmdQ'][0])
                    res.append(exCmd(qDicts[reNo]['cmdQ'][0], reNo))
                    qDicts[reNo]['cmdQ'] = qDicts[reNo]['cmdQ'][1:]
                   #print('reNo', reNo, ':', 'Commands left:', qGet(reNo))
                return (res, 'result_list')

        elif cmd == 'multi':
            qDicts[reNo]['charging'] = True
           #print('reNo', reNo, ':', 'Charging:')
            return('OK', 'simple_string')

        elif cmd == 'discard':
            if qDicts[reNo]['charging'] == True:
                defaultize(qDicts[reNo])
                keyWatchTimes[reNo] = {}
                return('OK','simple_string')
            else:
                return('ERR DISCARD without MULTI','simple_error')

        elif cmd == 'watch':
            if qDicts[reNo]['charging']:
                return ('ERR WATCH inside MULTI is not allowed', 'simple_error')
            else:
                '''
                wlst = list(inline[1:]) #in case len(inline) == 2) ???isn't
                there always just one key???
                '''
                i = inline[1]
                print(f'reNo {reNo}: now watching {i}')
                print(
                    f'keyWatchTimes[{reNo}] is currently'
                    f'{keyWatchTimes.get(reNo)}'
                    )
                keyWatchTimes[reNo][i] = time.time()
            return('OK','simple_string')
        elif cmd == 'unwatch':
            keyWatchTimes[reNo] = {}
            return('OK','simple_string')

        elif qDicts[reNo]['charging']:
            #print('reNo', reNo, ':', 'Still charging: adding', cmd, 'to', qGet(reNo))
           #print('reNo', reNo, ':', 'Still charging: adding', cmd, 'to', qDicts[reNo]['cmdQ'])
            qDicts[reNo]['cmdQ'].append(inline)
            #for i in qDicts: #print(f'qDict {i} is {qDicts[i]}')
            return('QUEUED', 'simple_string')

        elif cmd == 'echo':
            # returns what's after echo
            #outline = b'$'
            res = str(inline[1])
            return(res, 'bulk_string')
            #print('i is', i)
            curStr = str(i).encode("utf-8")
            outline += str(i).encode("utf-8")
            outline += b'\r\n'

        elif cmd == 'ping':
            return('PONG', 'simple_string')
            #outline = b'+PONG\r\n'

        #modifying varDict
        elif cmd == 'set':
            #print('varDict is', varDict)
            #outline = b'+OK\r\n'
            vName = inline[1]
            vVal = inline[2]
            if len(inline) > 3:
                if inline[3] and inline[4]:
                    # optional expiry parameters
                    oName = inline[3].lower()
                    oVal = int(inline[4])
                    #print('oName, oVal', oName, oVal)
                    if oName == 'px':
                        exps[vName] = datetime.now() + \
                            timedelta(microseconds=(oVal * 1000))
                        #print('will expire at', exps[vName])
                        #print('exps are', exps)
                    elif oName == 'ex':
                        exps[vName] = datetime.now() + \
                            timedelta(seconds=oVal)
            keyModTimes[vName] = time.time()
            varDict[vName] = vVal
            return ('OK', 'simple_string')

        elif cmd == 'get':
            vName = inline[1]
            vVal = varDict.get(vName)
            if vVal != None:
                if exps.get(vName) and exps.get(vName) < datetime.now():
                    #print(f'key {vName} expired')
                    return([], 'null_bulk_string')
                    #outline = b'$-1\r\n'
                    #print('exps are', exps)
                else:
                    vOut = str(vVal)
                    return(vOut, 'bulk_string')
                    outline = b'$' + \
                        l.encode("utf-8") + b'\r\n' + \
                        vOut.encode("utf-8") + b'\r\n'
            else:
                return([], 'null_bulk_string')
                #outline = b'$-1\r\n'

        #modifying varDict
        elif cmd == 'rpush':
            #print('before rpush, varDict is', varDict)
            listName = inline[1]  # making the list we add
            if varDict.get(listName) == None:
                keyModTimes[listName] = time.time()
                varDict[listName] = []
            if len(inline) == 3:
                listExtra = [inline[2]]
            else:
                listExtra = inline[2:]
                # adding the new part to the existing list
            l = len(varDict[listName]) + len(listExtra)
            #print(f'adding {listExtra} to {listName}, total length is {l}')
            keyModTimes[listName] = time.time()
            varDict[listName] += listExtra
            return(l, 'integer')
            #outline = b':' + str(l).encode("utf-8") + b'\r\n'

        #modifying varDict
        elif cmd == 'lpush':
            listName = inline[1]  # making the list we add
            if len(inline) == 3:
                listExtra = [inline[2]]
            else:
                listExtra = inline[:1:-1]
            if varDict.get(listName) != None:
                # adding the new part to the existing list
                keyModTimes[listName] = time.time()
                varDict[listName] = listExtra + varDict[listName]
            else:
                # if no list, make it
                keyModTimes[listName] = time.time()
                varDict[listName] = listExtra
            return(len(varDict[listName]), 'integer')
            #outline = b':' + str(l).encode("utf-8") + b'\r\n'

        elif cmd == 'lrange':
            listName = inline[1]
            if varDict.get(listName) == None:
                return([],'array')
                #outline = b'*0\r\n'
            else:
                tList = varDict.get(listName)
                #print('called list is', tList)
                n = len(tList)
                #print('its length is', n)
                inds = [int(j) for j in inline[2:4]]
                for i in range(2):
                    if inds[i] + n < 0:
                        inds[i] = 0
                    elif inds[i] < 0:
                        inds[i] = n + inds[i]
                #print('inds are', inds)
                tList = tList[inds[0]:inds[1]+1]
                return(tList, 'array')
                #outline = app.respParse.encode_out(tList)

        elif cmd == 'llen':
            listName = inline[1]
            tList = varDict.get(listName)
            if tList == None:
                return(0,'integer')
                outline = b':0\r\n'
            else:
                l = len(tList)
                return(len(tList), 'integer')
                #outline = b':' + str(l).encode("utf-8") + b'\r\n'

        #modifying varDict
        elif cmd == 'lpop':
            listName = inline[1]
            if varDict.get(listName) == None:
                return ([], 'null_array')
                #outline =  b'-1\r\n'
            if len(inline) <= 2:
                res = varDict[listName][0]
                keyModTimes[listName] = time.time()
                varDict[listName] = varDict[listName][1:]
                return(res,'unknown')
                outline = app.respParse.encode_out(
                    varDict[listName][0])
            else:
                k = int(inline[2])
                if len(varDict.get(listName)) <= k:
                    res = varDict[listName]
                    keyModTimes[listName] = time.time()
                    varDict[listName] = []
                    return(res, 'array')
                    #outline = app.respParse.encode_out( varDict[listName])
                else:
                    res = varDict[listName][:k]
                    keyModTimes[listName] = time.time()
                    varDict[listName] = varDict[listName][k:]
                    return(res, 'array')

        #modifying varDict
        elif cmd == 'blpop':  # todo: make commands into functions of string,
            # read the listname and timeout
            # literally just copying the rpush
            # todo just compile it together? if received a list, then listName is ...
            listName = inline[1]
            timeOut = float(inline[2])
            #print('timeout is', timeOut)
            # calculate when will the timeout expire, also getting a number
            tExp = time.time() + timeOut
            if waitstarts != []:
                waitcount = waitstarts[-1]+1
            else:
                waitcount = 0
            waitstarts.append(waitcount)
            #print('waitstarts is now', waitstarts)
            a = True
            chP = time.time()
            #print(f'waitcount: {waitcount}: first checkpoint is {chP}')
            while a:
                if timeOut != 0 and tExp < time.time():
                    #print(f'waitcount {waitcount}: expired')
                    a = 'expired'
                    waitstarts.remove(waitcount)
                    return([], 'null_array')
                a = (waitstarts[0] != waitcount) or (
                    not varDict.get(listName))
                if time.time() - chP > 0.4:
                    c = 0
                    #print(f'waitcount {waitcount}: prev chP {chP}, next checkpoint {time.time()} ')
                    chP = time.time()
                    # if not varDict.get(listName):
                    #print(f'waitcount {waitcount}: list {listName} still empty')
                    #print(f'look at it: {varDict.get(listName)}')
                    #print(f'and varDict is {varDict}')
                    # elif  (waitstarts[0] != waitcount):
                    #print(
                    #    f'Not my turn: my waitcount is {waitcount}, '
                    #    f'but waitstarts are {waitstarts}'
                    #      )
                    # else:
                    #print(f'waitcount {waitcount}: all conditions are satisfied')
            #print(f'waitcount {waitcount}: done with the loop')
            if a != 'expired':
                outlist = [listName, varDict[listName][0]]
                #outline = app.respParse.encode_out(outlist)
                keyModTimes[listName] = time.time()
                varDict[listName] = varDict[listName][1:]
                waitstarts.remove(waitcount)
                return(outlist, 'array')

        elif cmd == 'type':
            listName = inline[1]
            val = varDict.get(listName)
            if val == None:
                return('none', 'simple_string')
                #outline = app.respParse.enSimple('none')
            else:
                res = str(type(val))
                #print('value type is', res)
                res = res[8:-2]
                #print('res is', res)
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
                        #    #print('they are literally the same value')
                        # else:
                        #    comp = ''
                        #    #print(f'lengths are {len(res)} and {len(nes)}')
                        #    for i in range(len(res)):
                        #        if res[i] == nes [i]:
                        #            comp += res[i]
                        #        else:
                        #            comp += f'({res[i],nes[i]})'
                        #print('comp is', comp)
                        tip = 'unknown'
                return(tip,'simple_string')
                #outline = app.respParse.enSimple(tip)

        #modifying varDict
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
                #print('generating [0]')
                idVal[0] = int(1000 * time.time())
                #print('idVal[0] is', idVal[0])
                idVal.append('*')
            else:
                idVal[0] = int(idVal[0])
            if idVal[1] == '*':
                #print('generating[1]')
                if idVal[0] == idMin[0]:
                    idVal[1] = idMin[1] + 1
                else:
                    idVal[1] = 0
                #print('idVal[1] is', idVal[1])
            else:
                idVal[1] = int(idVal[1])
            #print('idVal is', idVal)
            streamID = '-'.join([str(x) for x in idVal])
            # now, let's validate:
            if max(idVal) <= 0:
                errMsg = 'ERR The ID specified in XADD must be greater than 0-0'
            elif idVal[0] < idMin[0] or (idVal[0] == idMin[0] and idVal[1] <= idMin[1]):
                errMsg = ('ERR The ID specified in XADD is equal or smaller than '
                          'the target stream top item')
            if errMsg:
                return(errMsg,'simple_error')
                       #outline = app.respParse.enErr(errMsg)
            else:
                if varDict.get(streamKey) == None:
                    #print('stream is new')
                    keyModTimes[streamKey] = time.time()
                    varDict[streamKey] = stream()
                # else:
                    #print('stream isn\'t new')
                keyModTimes[streamKey] = time.time()
                varDict[streamKey].idMin = idVal
                # todo get read of idMin, that's just the last element of ids
                keyModTimes[streamKey] = time.time()
                varDict[streamKey].ids.append(streamID)
                keyModTimes[streamKey] = time.time()
                varDict[streamKey].data[streamID] = {}
                for i in range(3, len(inline), 2): 
                    keyModTimes[i] = time.time()
                    varDict[streamKey].data[streamID][str(
                        inline[i])] = inline[i+1]
                #print(f'stream \'{streamKey}\' ids are now {varDict[streamKey].ids}')
                return(streamID, 'bulk_string')
                #outline = app.respParse.encode_out(streamID)

        elif cmd == 'xrange':
            #print('starting xrange')
            streamKey = inline[1]
            rB = inline[2]
            rE = inline[3]
            strm = varDict.get(streamKey)
            #outline = app.respParse.encode_out(
            return (strmGet(streamKey, rB, rE), 'array')

        elif cmd == 'xread':
            excl = True
            #print('starting xrange')
            if inline[1] == 'block':
                timeOut = int(inline[2])
                if timeOut == 0:
                    timeExp = True
                else:
                    timeExp = time.time() + timeOut/1000
                i = 4
                #print('timeOut is', timeOut, 'timeExp is', timeExp)
            else:
                timeExp = False
                i = 2
            keys = []
            #print(f'i is {i}')
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
                return(res,'array')
                #outline = app.respParse.encode_out(res)
            else:
                return([],'null_array')
                #outline = b'*-1\r\n'
                # todo --- encode empty list like that?
           #print('starting xrange')

        #modifying varDict
        elif cmd == 'incr':
            varKey = inline[1]
            #print('varKey is', varKey)
            #print('variable is', varDict.get(varKey))
            isInt = True
            if varDict.get(varKey) == None:
                #print('creating variable', varKey)
                keyModTimes[varKey] = time.time()
                varDict[varKey] = 1
                res = 1
                return(res,'integer')
                #outline = app.respParse.encode_out(res)
            else:
                try:
                    varDict[varKey] = int(varDict[varKey])
                    keyModTimes[varKey] = time.time()
                except:
                    isInt = False
                    #print('Error: the key is not numeric')
                    return('ERR value is not an integer or out of range','simple_error')
                if isInt:
                    keyModTimes[varKey] = time.time()
                    varDict[varKey] += 1
                    res = varDict[varKey]
                    #print('res =', res)
                    return(res,'integer')
                    #outline = app.respParse.encode_out(res)


        else:
            return('ERR Unknown command', 'simple_error')
            #outline = data

    def respond(conn):
        dataCopy = {}
        #logging the client id
        if responds == []:
            reNo = 1
        else:
            reNo = responds[-1] + 1
        responds.append(reNo)
        #main loop
        while True:
            data = conn.recv(4096)
            if data:
                #timeIn = datetime.now()
                connFD = connection.fileno()
                inline = app.respParse.decode_resp(data)
                print('inline is', inline)
                if inline[0] in dMod:
                    for i in varDict: #for i in keyWatchTimes:
                        dataCopy[i] = varDict[i]
                res = exCmd(inline, reNo)
                if dataCopy:
                    for i in dataCopy:
                        if dataCopy[i] != varDict[i]:
                            keyModTimes[i] = time.time()
                dataCopy = {}
                outline = app.respParse.encode_out(res)
                conn.send(outline)
               #print('sent outline is', outline)

            # conn.sendall(b"+PONG\r\n") #key part --- there must be a loop in this function

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        if not connection:
            break

       ##print('its type is', type(getConn))
       # for i in getConn:
            #print(f'getConn has element {i}')

        #print('connection\'s fd is ', socket.recv_fds(connection, 1024, 1024))
        # clients have different 'fd'? 'connection, addr' is not preserved
        thr = threading.Thread(target=respond, args=(connection,))
        #if thr: #print(thr)
        thr.start()

if __name__ == "__main__":
    # todo why the underscores? what does this specific initialization do?
    main()

# is there ever need to access qDict of one client from another? I don't think
# so

#almost any dict[reNo] can just be this client's local dict

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
from secrets import choice #to generate random IDs for replication extension
import time
import app.respParse
import argparse #to connect to a different port
from base64 import b64decode

### Constants ###
empty_rdb64 = 'UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=='
rdb_bin = b64decode(empty_rdb64) 
writeCommands = ['set', 'del']

parser = argparse.ArgumentParser()
parser.add_argument("--port", help="Connection port")
parser.add_argument("--replicaof", help="Whose replica")
args = parser.parse_args()
#'random' ID 


def main():
    #consts
    dMod = ('incr', 'xadd', 'blpop', 'lpop', 'lpush', 'rpush', 'exec')

    clIDs = []
    exps = {}
    waitstarts = []
    responds={} #client ids essentially
    varDict = {}
    repliDict = {}
    qDicts = {} #{'charging' : False, 'cmdQ' : []}
    

    '''
    all of this kinda sucks
    how about mxn table? keys, clids
    -1 bad, 1 watched, 0 nothing
    okay, you give a try to what you have now, and if not --- do the keyTrack
    matrix
    ...
    yeah, making sure everyone is everywhere only once is garbage
    '''

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

    class keyTrackMatrix:
        def __init__(self):
            self.keyNo = 0
            self.clNo = 0
            self.table = []
            self.keyndex = {}
        def addKey(self, key):
            if self.keyndex.get(key) != None:
                return
            self.keyndex[key] = self.keyNo
            self.keyNo += 1
            self.table.append([0]*self.clNo)
        def addClient(self):
            self.clNo += 1
            for i in range(self.keyNo):
                self.table[i].append(0)
        def setWatch(self, client_ind, key):
            self.addKey(key)
            print(
                    f'key is {key}, keyndex is {self.keyndex},'
                    f'and client index is {client_ind},'
                    f'the table is {self.table}'
                 )
            self.table[self.keyndex[key]][client_ind] = 1
        def modKey(self,key):
            if self.keyndex.get(key) != None:
                self.table[self.keyndex[key]] = [-abs(i) for i in self.table[self.keyndex[key]]]
            else:
                self.addKey(key)
        def clearClient(self,client_ind):
            for i in range(self.keyNo):
                self.table[i][client_ind] = 0
        def watchFailed(self,client_ind):
            print('table is', self.table)
            print(f'keyndex is {self.keyndex}')
            res = False
            for i in self.keyndex:
                if self.table[self.keyndex[i]][client_ind] == -1:
                    print(f'key {i} was changed under reNo {client_ind}\'s watch')
                    self.clearClient(client_ind)
                    res = True
            return res

    def random_id(l):
        res = ''
        ords = list(range(48, 58)) + list(range(97, 123))
        for i in range(l):
            res += chr(choice(ords))
        return res





    kematri = keyTrackMatrix()

    def sendCmd(result, connection):
        outline = app.respParse.encode_out((result, 'array'))
        #print(f'outline is {outline}')
        connection.send(outline)

    def waitFor(phrase, connection):
        data = None
        c = 1
        while data != phrase:
            prevdata = data
            data = app.respParse.decode_resp(connection.recv(4096))
            #if data != prevdata:
                #print(f'data changed to {data}')
        data = ''
        #print(f'wait for {phrase} has ended')
        return

    def handshake(replargs):
        #todo wow hacky make a function and call both when handshaking and respondsing
    
        if responds.get(portNo) == None:
            subNo = 1
            responds[portNo] = [1]
        else:
            subNo = responds[portNo][-1] + 1
    
        clID = str(portNo) + '-' + str(subNo)
        reNo = len(clIDs)
        clIDs.append(clID)
    
        print(f'responds is {responds}') 
        print(f'clIDs is {clIDs}')
    
        repliDict[reNo] = repliInfo


        o = replargs.split(' ')
        print(f'replargs is {o}')
        ownedby = (o[0], int(o[1]))
        print(f'ownedby is {ownedby}')
        repliDict[reNo]['ownedby'] = ownedby
        print(f'from replica: repliDict is {repliDict}')
        master_connection = socket.create_connection((ownedby[0], int(ownedby[1])))
        print(f'master_connection is {master_connection}')
    
        sendCmd('PING', master_connection)
        waitFor('PONG', master_connection)
    
    
        sendCmd('REPLCONF listening-port ' + str(portNo), master_connection)
        waitFor('OK', master_connection)
        sendCmd('REPLCONF capa psync2', master_connection)
        waitFor('OK', master_connection)
        sendCmd('PSYNC ? -1', master_connection)
        #print(f'repliDict[reNo] is {repliDict[reNo]}')

        #todo --- slaveInit, masterInit?
        #todo --- sendwait? unite both
 

    def exCmd(inline, reNo, sender = None):
        print(f'reNo {reNo}: executing {inline[0].upper()}')
        #print('exCmd thinks that reNo is', reNo)
        if qDicts.get(reNo) == None:
            qDicts[reNo] = {}
            defaultize(qDicts[reNo])

        #print('reNo', reNo, ':', 'qDict is', qDicts[reNo])
        #exCmd sees it, until I refer to it from respond. hm.
        if type(inline) == list:
            cmd = inline[0].lower()
        else:
            cmd = inline

        if cmd == 'exec':
            qDict = {}
            for i in qDicts[reNo]:
                qDict[i] = qDicts[reNo][i]
            defaultize(qDicts[reNo])
            #print(f'qDict is', qDict)
            if kematri.watchFailed(reNo):
                    return ('', 'null_array')
            #print('reNo', reNo, ':', 'Casting:') #print('reNo', reNo, ': qDict is', qDicts.get(reNo))
            if not qDict['charging']:
                return('ERR EXEC without MULTI', 'simple_error')
                #outline = app.respParse.enErr('ERR EXEC without MULTI')
            qDict['charging'] = False
            if not qDict['cmdQ']:
                return([],'array')
                #outline = app.respParse.encode_out([])
            else:
                res = []
                while qDict['cmdQ']:
                   #print('reNo', reNo, ':', 'Casting', qDict['cmdQ'][0])
                    res.append(exCmd(qDict['cmdQ'][0], reNo))
                    qDict['cmdQ'] = qDict['cmdQ'][1:]
                   #print('reNo', reNo, ':', 'Commands left:', qGet(reNo))
                return (res, 'result_list')

        elif cmd == 'multi':
            qDicts[reNo]['charging'] = True
           #print('reNo', reNo, ':', 'Charging:')
            return('OK', 'simple_string')

        elif cmd == 'discard':
            if qDicts[reNo]['charging'] == True:
                defaultize(qDicts[reNo])
                kematri.clearClient(reNo)
                return('OK','simple_string')
            else:
                return('ERR DISCARD without MULTI','simple_error')

        elif cmd == 'watch':
            if qDicts[reNo]['charging']:
                return ('ERR WATCH inside MULTI is not allowed', 'simple_error')
            else:
                toWatch = list(inline[1:])
                #print('reNo is', reNo)
                for i in toWatch:
                    #print(f'reNo {reNo}+1: watching key {i}')
                    kematri.setWatch(reNo, i)
            return('OK','simple_string')

        elif cmd == 'unwatch':
            kematri.clearClient(reNo)
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
            kematri.modKey(vName)
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
                kematri.modKey(listName)
                varDict[listName] = []
            if len(inline) == 3:
                listExtra = [inline[2]]
            else:
                listExtra = inline[2:]
                # adding the new part to the existing list
            l = len(varDict[listName]) + len(listExtra)
            #print(f'adding {listExtra} to {listName}, total length is {l}')
            kematri.modKey(listName)
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
                kematri.modKey(listName)
                varDict[listName] = listExtra + varDict[listName]
            else:
                # if no list, make it
                kematri.modKey(listName)
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
                kematri.modKey(listName)
                varDict[listName] = varDict[listName][1:]
                return(res,'unknown')
                outline = app.respParse.encode_out(
                    varDict[listName][0])
            else:
                k = int(inline[2])
                if len(varDict.get(listName)) <= k:
                    res = varDict[listName]
                    kematri.modKey(listName)
                    varDict[listName] = []
                    return(res, 'array')
                    #outline = app.respParse.encode_out( varDict[listName])
                else:
                    res = varDict[listName][:k]
                    kematri.modKey(listName)
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
                kematri.modKey(listName)
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
                    kematri.modKey(streamKey)
                    varDict[streamKey] = stream()
                # else:
                    #print('stream isn\'t new')
                kematri.modKey(streamKey)
                varDict[streamKey].idMin = idVal
                # todo get read of idMin, that's just the last element of ids
                kematri.modKey(streamKey)
                varDict[streamKey].ids.append(streamID)
                kematri.modKey(streamKey)
                varDict[streamKey].data[streamID] = {}
                for i in range(3, len(inline), 2): 
                    kematri.modKey(i)
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
                kematri.modKey(varKey)
                varDict[varKey] = 1
                res = 1
                return(res,'integer')
                #outline = app.respParse.encode_out(res)
            else:
                try:
                    varDict[varKey] = int(varDict[varKey])
                    kematri.modKey(varKey)
                except:
                    isInt = False
                    #print('Error: the key is not numeric')
                    return('ERR value is not an integer or out of range','simple_error')
                if isInt:
                    kematri.modKey(varKey)
                    varDict[varKey] += 1
                    res = varDict[varKey]
                    #print('res =', res)
                    return(res,'integer')
                    #outline = app.respParse.encode_out(res)

        elif cmd == 'info':
            arg = inline[1]
            res = ''
            if arg == 'replication':
                print(f'repliDict[reNo] is {repliDict[reNo]}')
                for i in repliDict[reNo]:
                    res += i +':' + str(repliDict[reNo][i]) + '\n' 
                print(f'res is {res}')
            return(res,'bulk_string')

        elif cmd == 'replconf':
            #master_connection = socket.create_connection((ownedby[0], int(ownedby[1])))
            return('OK','simple_string')

        elif cmd == 'psync':
            res1 = 'FULLRESYNC ' + str(repliDict[reNo]['master_replid']) + ' ' + str(0)
            #print(f'psync1 result is {res1}') 
            res2 = rdb_bin
            #print(f'psync2 result is {res2}, converted from {empty_rdb64}')
            res = [(res1, 'simple_string'), (res2, 'rdb')]
            if repliDict[reNo].get('replicae') == None:
                repliDict[reNo]['replicae'] = [sender]
            elif not sender in repliDict[reNo]['replicae']:
                repliDict[reNo]['replicae'].append(sender)
            return(res, 'result_sequence')

        else:
            return('ERR Unknown command', 'simple_error')
            #outline = data
   
    
    def respond(conn, role):
        print(f'connection is {conn}')

        print(f'responds is {responds}')
        if responds.get(portNo) == None:
            subNo = 1
            responds[portNo] = [1]
        else:
            subNo = responds[portNo][-1] + 1
            responds[portNo].append(subNo)
    
        clID = str(portNo) + '-' + str(subNo)
        reNo = len(clIDs)
        clIDs.append(clID)
    
        print(f'responds is {responds}') 
        print(f'clIDs is {clIDs}')
    
        repliDict[reNo] = repliInfo



        #print(f'responds[portNo] are {responds.get(portNo)}')
        kematri.addClient()
        #print('connection is', conn, 'reNo is', reNo)

        #main loop
        while True:
            data = conn.recv(4096)
            if data:
                #timeIn = datetime.now()
                inline = app.respParse.decode_resp(data)
                print(f'reNo {reNo}: inline is', inline)
                res = exCmd(inline, reNo, conn)
                reps = repliDict[reNo].get('replicae') 
                if type(inline) == list: #todo this should only be done once
                    cmd = inline[0].lower()
                else:
                    cmd = inline
                if reps != None and cmd in writeCommands: #todo wow hacky, need list of propagatables
                    #actually, need a command class, and propagatable attribute
                    for i in reps:
                        propagated_command = ' '.join(inline)
                        print('sending', propagated_command, 'to', i)
                        sendCmd(propagated_command, i)
                # if master -- need to keep track of the replica connections
                # after exCmd --- need to propagate
                outline = app.respParse.encode_out(res)
                if not isinstance(outline, list):
                    outline = [outline]
                for i in outline: #for the commands that send multiple messages
                    #print('sending', i)
                    conn.send(i)


    #######################
    #parsing arguments
    #######################

    if args.port:
        portNo = int(args.port)
    else:
        portNo = 6379

    repliInfo = {}

    if args.replicaof:
        print('initially, the replicaof args are', args.replicaof)
        role = repliInfo['role'] = 'slave'
        thr = threading.Thread(target=handshake, args=(args.replicaof,))
        thr.start()
    else:
        role = repliInfo['role'] = 'master'
        repliInfo['master_replid'] = random_id(40)
        repliInfo['master_repl_offset'] = 0
        repliInfo['replicas'] = []

    

        #outline = app.respParse.encode_out(('PING', 'array'))
        #print(f'outline is {outline}')
        #slocket.send(outline)

    server_socket = socket.create_server(("localhost", portNo), reuse_port=True)





    while True:
        connection, _ = server_socket.accept()
        if not connection:
            break
        thr = threading.Thread(target=respond, args=(connection,role,))
        thr.start()
    
    
if __name__ == "__main__":
    # todo why the underscores? what does this specific initialization do?
    main()

#exec
#multi
#discard
#watch
#unwatch
#echo
#ping
#set
#get
#rpush
#lpush
#lrange
#llen
#lpop
#blpop
#type
#xadd
#xrange
#xread
#incr
#info
#replconf
#psync

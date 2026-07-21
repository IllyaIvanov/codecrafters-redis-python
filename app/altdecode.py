
import re
import respParse

line = b'*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'
# +-:$*
prefix_pattern = b'[$:+\-*]\d*\\r\\n'
#prefix_separator = re.compile(prefix_pattern)
#prefix_matches = prefix_separator.findall(line)
#print(f'prefix_matches is {prefix_matches} \n')


command_list =[b'RPUSH', b'PING', b'GET', b'SET', b'LRANGE', b'ECHO', b'LPUSH',
               b'LPOP', b'LLEN', b'TYPE', b'XADD', b'XRANGE', b'XREAD', b'MULTI',
               b'EXEC', b'INCR', b'DISCARD', b'WATCH', b'REPLCONF', b'INFO', b'PSYNC',
               b'FULLRESYNC']

command_pattern =b'(?:' +  b'|'.join(command_list) + b')'

#print(f'command_pattern is {command_pattern}')
complete_pattern = b'(' + prefix_pattern + command_pattern + b')'
#print(f'complete_pattern is {complete_pattern}')
command_separator = re.compile(complete_pattern) #regexp pattern


matches = command_separator.findall(line)
#print(f'matches is {matches} \n')

#parts = command_separator.split(line)
parts = line.split(b'\r\n')
print(f'parts is {parts}\n')


inline = respParse.decode_resp(line)
print('inline: ', inline, '\n')
print('\n')

#def messingwithregex():
#    #patt_excl = re.compile(b'[+*-:$]') #regexp pattern
#    #excline = re.split(patt_excl,line)
#    #print('excline: ', excline)
#    
#    patt_inc = re.compile(b'([+*-:$]\d*)') #regexp pattern
#    line = b'*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n'
#    
#    incline = re.split(patt_inc,line)
#    print('incline: ', incline)
#    
#
#def messing_with_rdb(): 
#    from base64 import b16decode, b64decode
#    rdb_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
#    
#    rdb_64= 'UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=='
#    
#    rdb_bin = b64decode(rdb_64)
#    print(rdb_bin)
#
##while len(rdb_hex) > 0:
##    next_hex = rdb_hex[0:2]
##    next_ord = int(next_hex, 64)
##    ordstr = ' '*(3-len(str(next_ord))) + str(next_ord)
##    next_char = chr(next_ord)
##    print(next_hex, ordstr , next_char)
##    rdb_bin += next_char
##    rdb_hex = rdb_hex[2:]
##print(f'rdb_hex is {rdb_hex}') 
#
#
#
##a = 'asdf'
##b = a.split(' ')
##print(b)
#
##import argparse #to connect to a different port
##
##parser = argparse.ArgumentParser()
##parser.add_argument("--port", help="Connection port")
##args = parser.parse_args()
##
##
##print('it passed', args.port)
#
## 2026 May 16, 13:34
## learning isnumeric
##a = 'p3o5bug42h083r'
##for i in a:
##    if i.isnumeric():
##        print(f'character {i} is numeric')
##    else:
##        print(f'character {i} isn\'t numeric')

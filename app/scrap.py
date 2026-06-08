from base64 import b16decode, b64decode
rdb_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'

rdb_64= 'UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=='

rdb_bin = b64decode(rdb_64)
print(rdb_bin)

#while len(rdb_hex) > 0:
#    next_hex = rdb_hex[0:2]
#    next_ord = int(next_hex, 64)
#    ordstr = ' '*(3-len(str(next_ord))) + str(next_ord)
#    next_char = chr(next_ord)
#    print(next_hex, ordstr , next_char)
#    rdb_bin += next_char
#    rdb_hex = rdb_hex[2:]
#print(f'rdb_hex is {rdb_hex}') 



#a = 'asdf'
#b = a.split(' ')
#print(b)

#import argparse #to connect to a different port
#
#parser = argparse.ArgumentParser()
#parser.add_argument("--port", help="Connection port")
#args = parser.parse_args()
#
#
#print('it passed', args.port)

# 2026 May 16, 13:34
# learning isnumeric
#a = 'p3o5bug42h083r'
#for i in a:
#    if i.isnumeric():
#        print(f'character {i} is numeric')
#    else:
#        print(f'character {i} isn\'t numeric')

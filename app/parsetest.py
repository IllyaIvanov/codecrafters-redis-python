import respParse

print('\n\n\n')
print('\n\n\n')

line =  b'+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n' 

dec = respParse.decode_resp(line)

print(dec)
## lines = []
## fails = []
## 
## with open("datasamples.txt", "r") as f:
##     for line in f:
##         print('read line', line)
##         l1 = line.encode('ISO-8859-1')
##         print(f'l1 is {l1}')
##         actual_line = l1.decode('unicode-escape').encode('ISO-8859-1')[:-1]
## 
##         print(f'actual_line is {actual_line}')
##         lnew = respParse.alt_decode(actual_line) 
##         lold = respParse.decode_resp(actual_line)
##         if lnew == lold:
##             print(f'passed')
##         else:
##             print('failed')
##             fails.append((line, lold, lnew))
## 
## 
## print(f'got {len(fails)} fails')
## print(fails)
## 
## for f in fails:
##     print(f'line {f[0]} failed:') 
##     print(f'lold is {f[1]}')
##     print(f'lnew is {f[2]}\n') 
##     

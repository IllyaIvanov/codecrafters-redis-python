import respParse

print('\n\n\n')
print('\n\n\n')

lines = []
fails = []

with open("datasamples.txt", "r") as f:
    for line in f:
        print('read line', line)
        l1 = line.encode('ISO-8859-1')
        print(f'l1 is {l1}')
        actual_line = l1.decode('unicode-escape').encode('ISO-8859-1')[:-1]

        print(f'actual_line is {actual_line}')
        lnew = respParse.alt_decode(actual_line) 
        lold = respParse.decode_resp(actual_line)
        if lnew == lold:
            print(f'passed')
        else:
            print('failed')
            fails.append((line, lold, lnew))


print(f'got {len(fails)} fails')
print(fails)

for f in fails:
    print(f'line {f[0]} failed:') 
    print(f'lold is {f[1]}')
    print(f'lnew is {f[2]}\n') 
    

# 2026 May 16, 13:34
# learning isnumeric
a = 'p3o5bug42h083r'
for i in a:
    if i.isnumeric():
        print(f'character {i} is numeric')
    else:
        print(f'character {i} isn\'t numeric')

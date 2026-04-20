import socket
line = repr(b'*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n')
print(line)
data = line.split(b'r\n\')
print(data)
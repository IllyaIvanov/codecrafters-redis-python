import socket
line = b'*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n'
print(line)
data = line.split(b'\r\n')
print(data[2][1:])
print(data[2].decode("utf-8"))
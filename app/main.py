import socket  # noqa: F401
import threading


def main():
    def respIn(inline):
        prefix = chr(inline[0]) #chr -- converts single byte char to actual char
        if prefix == '+': #simple string
            return
        elif prefix =='-': #error
            return
        elif prefix ==':': #int
            return
        elif prefix =='$': #bulk string
            return
        elif prefix =='*': #array
            res = []
            lines = inline.split(b'\r\n')
            print(lines)
            count = int(lines[0][1:])
            for i in range(count):
                res.append(lines[2*i+2].decode("utf-8"))
        return res

    def respond(conn):
        while True:
            data = conn.recv(1024)
            if data:
                inline = respIn(data)
                if type(inline) == list:
                    outline = b'$' + str(len(inline)).encode("utf-8") +b'\r\n'
                    for i in inline:
                        outline += str(i).encode("utf-8") 
                        outline += b'\r\n'
                print(outline)
                conn.send(outline)
    
            #conn.sendall(b"+PONG\r\n") #key part --- there must be a loop in this function

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept() # _ is used because .accept() returns two values
        if not connection: 
            break
        thr = threading.Thread(target=respond, args=(connection,))
        thr.start()
        
        #for curcon in connections:
        #    data = curcon.recv(1024)
        #    if data:
        #        curcon.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()

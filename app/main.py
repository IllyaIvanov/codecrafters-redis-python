import socket  # noqa: F401
import threading


def main():
    def respParse(str):
        if str[0] == '+': #simple string
            return
        elif str[0] =='-': #error
            return
        elif str[0] ==':': #int
            return
        elif str[0] =='$': #bulk string
            return
        elif str[0] =='*': #array
            return

    def respond(conn):
        while True:
            data = conn.recv(1024)
            if data:
                #data = data.decode("utf-8")
                print('data is', data)
                data = repr(data)
                print('raw data is',data)
                rawwords = repr(data.decode("utf-8"))
                print('raw words are', rawwords)
#                outline = str(len(data)) + '\\r\\n' + data +'\\r\\n'
#                print('outline)
                outline=''
                conn.send(outline.encode("utf-8"))
    
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

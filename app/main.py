import socket  # noqa: F401
import threading


def main():
    def respond(conn):
        data = conn.recv(1024)
        if data:
            conn.sendall(b"+PONG\r\n")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    threads = []
    while True:
        connection, _ = server_socket.accept() # _ is used because .accept() returns two values
        if connection:
            thr = threading.Thread(target=respond, args=(connection))
            threads.append(thr)
            thr.start
        
        #for curcon in connections:
        #    data = curcon.recv(1024)
        #    if data:
        #        curcon.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()

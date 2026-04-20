import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept() # _ is used because .accept() returns two values
    #connection, shm = server_socket.accept() # _ is used because .accept() returns two values
    #print(connection, shm) #the .accept() doesn't happen on it's own, makes sense
    while True:
        data = connection.recv(1024)
        if data:
            connection.sendall(b"+PONG\r\n")
            data = None
            break


if __name__ == "__main__":
    main()

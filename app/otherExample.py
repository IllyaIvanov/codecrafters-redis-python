import socket
import threading
import time

store = {}
expiry = {}
streams = {}
lists = {}

blocking_xread = []
blocking_blpop = []


# =========================
# RESP ENCODERS
# =========================


def encode_simple(value):
    return f"+{value}\r\n".encode()


def encode_bulk(value):
    if value is None:
        return b"$-1\r\n"

    return f"${len(str(value))}\r\n{value}\r\n".encode()


def encode_integer(value):
    return f":{value}\r\n".encode()


def encode_error(value):
    return f"-ERR {value}\r\n".encode()


def encode_array(arr):
    if arr is None:
        return b"*-1\r\n"

    out = f"*{len(arr)}\r\n"

    for item in arr:
        if isinstance(item, list):
            out += encode_array(item).decode()

        elif isinstance(item, int):
            out += encode_integer(item).decode()

        elif item is None:
            out += "$-1\r\n"

        else:
            out += encode_bulk(str(item)).decode()

    return out.encode()


# =========================
# RESP PARSER
# =========================


def parse_resp(data):
    parts = data.decode().split("\r\n")

    args = []
    i = 0

    while i < len(parts):
        if parts[i].startswith("$"):
            args.append(parts[i + 1])
            i += 2
        else:
            i += 1

    return args


# =========================
# STREAM HELPERS
# =========================


def compare_ids(a, b):
    a1, a2 = map(int, a.split("-"))
    b1, b2 = map(int, b.split("-"))

    return (a1, a2) > (b1, b2)


def get_stream_entries(stream_key, start_id):
    result = []

    if stream_key not in streams:
        return result

    for entry_id, fields in streams[stream_key]:
        if compare_ids(entry_id, start_id):
            result.append([entry_id, fields])

    return result


# =========================
# CLIENT HANDLER
# =========================


def handle_client(client):
    in_multi = False
    transaction_queue = []

    while True:
        try:
            data = client.recv(4096)

            if not data:
                break

            args = parse_resp(data)

            if not args:
                continue

            cmd = args[0].upper()

            # ========================
            # EXEC
            # ========================
            if cmd == "EXEC":
                if not in_multi:
                    client.send(b"-ERR EXEC without MULTI\r\n")
                    continue

                # Empty transaction
                if len(transaction_queue) == 0:
                    client.send(b"*0\r\n")
                    in_multi = False
                    continue

            # ========================
            # MULTI
            # ========================
            if cmd == "MULTI":
                in_multi = True
                transaction_queue = []
                client.send(b"+OK\r\n")
                continue

            # Queue commands while inside MULTI
            if in_multi:
                transaction_queue.append(args)
                client.send(b"+QUEUED\r\n")
                continue

            # =========================
            # PING
            # =========================
            if cmd == "PING":
                client.send(encode_simple("PONG"))

            # =========================
            # ECHO
            # =========================
            elif cmd == "ECHO":
                client.send(encode_bulk(args[1]))

            # =========================
            # SET
            # =========================
            elif cmd == "SET":
                key = args[1]
                value = args[2]

                store[key] = value

                if len(args) > 4 and args[3].upper() == "PX":
                    expiry[key] = time.time() + (int(args[4]) / 1000)

                client.send(encode_simple("OK"))

            # =========================
            # GET
            # =========================
            elif cmd == "GET":
                key = args[1]

                if key in expiry and time.time() > expiry[key]:
                    del store[key]
                    del expiry[key]

                value = store.get(key)

                client.send(encode_bulk(value))

            # ========================
            # INCR
            # ========================
            elif cmd == "INCR":
                key = args[1]

                # key doesn't exist
                if key not in store:
                    store[key] = "1"
                    client.send(encode_integer(1))
                    continue

                value = store[key]

                # value must be integer
                try:
                    num = int(value)
                except:
                    client.send(b"-ERR value is not an integer or out of range\r\n")
                    continue

                num += 1
                store[key] = str(num)

                client.send(encode_integer(num))

            # =========================
            # TYPE
            # =========================
            elif cmd == "TYPE":
                key = args[1]

                if key in streams:
                    client.send(encode_simple("stream"))

                elif key in lists:
                    client.send(encode_simple("list"))

                elif key in store:
                    client.send(encode_simple("string"))

                else:
                    client.send(encode_simple("none"))

            # =========================
            # RPUSH
            # =========================
            elif cmd == "RPUSH":
                key = args[1]
                values = args[2:]

                if key not in lists:
                    lists[key] = []

                lists[key].extend(values)

                client.send(encode_integer(len(lists[key])))

                # unblock BLPOP
                for waiter in blocking_blpop[:]:
                    wait_key, wait_client = waiter

                    if wait_key == key and lists[key]:
                        value = lists[key].pop(0)

                        wait_client.send(encode_array([key, value]))

                        blocking_blpop.remove(waiter)
                        break

            # =========================
            # LPUSH
            # =========================
            elif cmd == "LPUSH":
                key = args[1]
                values = args[2:]

                if key not in lists:
                    lists[key] = []

                for v in values:
                    lists[key].insert(0, v)

                client.send(encode_integer(len(lists[key])))

            # =========================
            # LPOP
            # =========================
            elif cmd == "LPOP":
                key = args[1]

                if key not in lists or len(lists[key]) == 0:
                    client.send(b"$-1\r\n")
                    continue

                # LPOP key count
                if len(args) > 2:
                    count = int(args[2])

                    popped = []

                    for _ in range(min(count, len(lists[key]))):
                        popped.append(lists[key].pop(0))

                    client.send(encode_array(popped))

                else:
                    value = lists[key].pop(0)
                    client.send(encode_bulk(value))

            # =========================
            # LLEN
            # =========================
            elif cmd == "LLEN":
                key = args[1]

                if key not in lists:
                    client.send(encode_integer(0))
                else:
                    client.send(encode_integer(len(lists[key])))

            # =========================
            # LRANGE
            # =========================
            elif cmd == "LRANGE":
                key = args[1]
                start = int(args[2])
                end = int(args[3])

                if key not in lists:
                    client.send(encode_array([]))
                    continue

                lst = lists[key]

                if end == -1:
                    end = len(lst) - 1

                result = lst[start : end + 1]

                client.send(encode_array(result))

            # =========================
            # BLPOP
            # =========================
            elif cmd == "BLPOP":
                key = args[1]
                timeout = float(args[2])

                if key in lists and lists[key]:
                    value = lists[key].pop(0)

                    client.send(encode_array([key, value]))

                else:
                    if timeout == 0:
                        blocking_blpop.append((key, client))

                    else:
                        start = time.time()

                        found = False

                        while time.time() - start < timeout:
                            if key in lists and lists[key]:
                                value = lists[key].pop(0)

                                client.send(encode_array([key, value]))

                                found = True
                                break

                            time.sleep(0.01)

                        if not found:
                            client.send(b"*-1\r\n")

            # =========================
            # XADD
            # =========================
            elif cmd == "XADD":
                stream_key = args[1]
                entry_id = args[2]
                fields = args[3:]

                if stream_key not in streams:
                    streams[stream_key] = []

                # AUTO ID
                if entry_id == "*":
                    ms = int(time.time() * 1000)

                    seq = 0

                    if streams[stream_key]:
                        last_id = streams[stream_key][-1][0]
                        last_ms, last_seq = map(int, last_id.split("-"))

                        if last_ms == ms:
                            seq = last_seq + 1

                    entry_id = f"{ms}-{seq}"

                # PARTIAL AUTO ID
                elif entry_id.endswith("-*"):
                    ms = int(entry_id.split("-")[0])

                    seq = 0

                    existing = [
                        int(x[0].split("-")[1])
                        for x in streams[stream_key]
                        if int(x[0].split("-")[0]) == ms
                    ]

                    if existing:
                        seq = max(existing) + 1

                    elif ms == 0:
                        seq = 1

                    entry_id = f"{ms}-{seq}"

                # NORMAL IDS
                else:
                    ms, seq = map(int, entry_id.split("-"))

                    if ms == 0 and seq == 0:
                        client.send(
                            encode_error(
                                "The ID specified in XADD must be greater than 0-0"
                            )
                        )
                        continue

                    if streams[stream_key]:
                        last_id = streams[stream_key][-1][0]
                        last_ms, last_seq = map(int, last_id.split("-"))

                        if (ms, seq) <= (last_ms, last_seq):
                            client.send(
                                encode_error(
                                    "The ID specified in XADD is equal or smaller than the target stream top item"
                                )
                            )
                            continue

                streams[stream_key].append((entry_id, fields))

                client.send(encode_bulk(entry_id))

                # unblock XREAD
                for waiter in blocking_xread[:]:
                    wait_streams, wait_ids, wait_client = waiter

                    response = []

                    for i in range(len(wait_streams)):
                        s = wait_streams[i]
                        start_id = wait_ids[i]

                        entries = get_stream_entries(s, start_id)

                        if entries:
                            response.append([s, entries])

                    if response:
                        wait_client.send(encode_array(response))
                        blocking_xread.remove(waiter)

            # =========================
            # XRANGE
            # =========================
            elif cmd == "XRANGE":
                stream_key = args[1]
                start = args[2]
                end = args[3]

                if stream_key not in streams:
                    client.send(encode_array([]))
                    continue

                result = []

                for entry_id, fields in streams[stream_key]:
                    if start != "-" and compare_ids(start, entry_id):
                        continue

                    if end != "+" and compare_ids(entry_id, end):
                        continue

                    result.append([entry_id, fields])

                client.send(encode_array(result))

            # =========================
            # XREAD
            # =========================
            elif cmd == "XREAD":
                block_time = None

                if args[1].lower() == "block":
                    block_time = int(args[2])
                    streams_index = 4
                else:
                    streams_index = 2

                stream_keys = []
                start_ids = []

                remaining = args[streams_index:]

                half = len(remaining) // 2

                stream_keys = remaining[:half]
                start_ids = remaining[half:]

                # HANDLE $
                for i in range(len(start_ids)):
                    if start_ids[i] == "$":
                        if stream_keys[i] in streams and streams[stream_keys[i]]:
                            start_ids[i] = streams[stream_keys[i]][-1][0]
                        else:
                            start_ids[i] = "0-0"

                response = []

                for i in range(len(stream_keys)):
                    entries = get_stream_entries(stream_keys[i], start_ids[i])

                    if entries:
                        response.append([stream_keys[i], entries])

                if response:
                    client.send(encode_array(response))

                else:
                    if block_time is None:
                        client.send(b"*-1\r\n")

                    elif block_time == 0:
                        blocking_xread.append((stream_keys, start_ids, client))

                    else:
                        start = time.time()

                        found = False

                        while time.time() - start < block_time / 1000:
                            response = []

                            for i in range(len(stream_keys)):
                                entries = get_stream_entries(
                                    stream_keys[i], start_ids[i]
                                )

                                if entries:
                                    response.append([stream_keys[i], entries])

                            if response:
                                client.send(encode_array(response))
                                found = True
                                break

                            time.sleep(0.01)

                        if not found:
                            client.send(b"*-1\r\n")

            else:
                client.send(encode_error("unknown command"))

        except Exception as e:
            print("ERROR:", e)
            break

    client.close()


# =========================
# MAIN
# =========================


def main():
    server = socket.create_server(("localhost", 6379), reuse_port=True)

    print("Server started...")

    while True:
        client, addr = server.accept()

        threading.Thread(target=handle_client, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()

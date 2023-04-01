import threading
from .raft import kv_server, transport


def main():
    kv_server_thread = threading.Thread(target=kv_server.main)
    raft_server_thread = threading.Thread(target=transport.main)

    kv_server_thread.start()
    raft_server_thread.start()

    kv_server_thread.join()
    raft_server_thread.join()


if __name__ == '__main__':
    main()

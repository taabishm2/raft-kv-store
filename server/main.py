import threading
from raft import kv_server, transport

def main():
    program1_thread = threading.Thread(target=kv_server.main)
    program2_thread = threading.Thread(target=transport.main)

    program1_thread.start()
    program2_thread.start()

    program1_thread.join()
    program2_thread.join()

if __name__ == '__main__':
    main()

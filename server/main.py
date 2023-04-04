import threading
import time
from os import environ

from .raft.election import election
from .raft import kv_server, transport


def main():
    for port in [(5440,4000)]:
        kv_server_thread = threading.Thread(target=kv_server.main, args=[port[0]])
        raft_server_thread = threading.Thread(target=transport.main, args=[port[1]])

        kv_server_thread.start()
        raft_server_thread.start()

    # TODO: remove this (test code)
    #print(f"*** WAITING 10 secs for servers, leader={environ['IS_LEADER']} ***")
    #time.sleep(5)
    #if environ['IS_LEADER'] == "TRUE": election.trigger_election()

    kv_server_thread.join()
    raft_server_thread.join()


if __name__ == '__main__':
    main()

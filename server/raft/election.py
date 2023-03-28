import enum
import os
import time

from queue import Queue
from threading import Lock, Thread
from raft import config as config

# from .config import config 
from .log_manager import *
from .transport import *


# Using enum class create enumerations
class NodeRole(enum.Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

class Election:
    def __init__(self, term, transport: Transport):
        self.timeout_thread = None
        self.role = self.get_init_role()
        self.term = term
        self.__transport = transport
        self.__lock = Lock()

        print(f"I am {self.role}")

    def get_init_role(self):
        is_leader = os.environ['IS_LEADER']
        if is_leader == "TRUE":
            return NodeRole.Leader

        return NodeRole.Follower

    def is_node_leader(self):
        return (self.role == NodeRole.Leader)

    def init_heartbeat(self):
        '''
        Initiate periodic heartbeats to the follower nodes if node is leader
        '''
        if self.role != NodeRole.Leader:
            return
        print(f"Node is leader for the term {self.term}")
        print('sending heartbeat to peers')
        # send heartbeat to peers once peers are added to transport
        # for peer in self.peers:
        #     Thread(target=self.send_heartbeat, args=(peer,)).start()

    def send_heartbeat(self, peer: str):
        '''
        SEND heartbeat to the 'peer' and get it's response if LEADER

        :param peer: address of the follower node
        :type peer: str
        '''
        try:
            message = {'term': self.term, 'addr': self.__transport.addr}
            while self.role == NodeRole.Leader:
                print(f'[PEER HEARTBEAT] {peer}')
                start = time.time()
                # reply = self.__transport.heartbeat(peer=peer, message=message)
                # if reply:
                #     # Peer has higher term. Relinquish leadership
                #     if reply['term'] > self.term:
                #         self.term = reply['term']
                #         self.role = NodeRole.Follower
                #         self.init_timeout()
                # delta = time.time() - start
                # time.sleep((config.HB_TIME - delta) / 1000)
                # print(f'[PEER HEARTBEAT RESPONSE] {peer} {reply}')
        except Exception as e:
            raise e

    def heartbeat_handler(self, message: dict) -> tuple:
        '''
        Handler function for the FOLLOWER node to validate the heartbeat RECIEVED
        from the leader

        :param message: heartbeat data as sent by the leader node
        :type message: dict

        :returns: term and latest commit_id of this (follower) node
        :rtype: tuple
        '''
        try:
            term = message['term']
            if self.term <= term:
                self.leader = message['addr']
                self.reset_timeout()
                print(f'got heartbeat from leader {self.leader}')
                if self.role == NodeRole.Candidate:
                    self.role = NodeRole.Follower
                elif self.role == NodeRole.Leader:
                    self.role = NodeRole.Follower
                    self.init_timeout()

                if self.term < term:
                    self.term = term

                if 'action' in message:
                    print(f'received command from leader {message}')
                    #  TODO: Do action in message
                    # self.store.action_handler(message)
            return self.term, 0 # self.store.commit_id TODO: SEND LATEST COMMIT ID
        except Exception as e:
            raise e

    def timeout_loop(self):
        '''
        if this node is not the leader, wait for the leader
        to send the heartbeat. If heartbeat is not received by the follower,
        within some unit time then start the election. This loop will
        run endlessly
        '''
        while self.role != NodeRole.Leader:
            delta = self.election_time - time.time()
            if delta < 0:
                # TODO: START ELECTION!
                print("TODO")
            else:
                time.sleep(delta)

    def init_timeout(self):
        '''
        Checks for missed heartbeats from the leader and start the election
        '''
        try:
            print('Starting timeout')
            self.reset_timeout()
            if self.timeout_thread and self.timeout_thread.is_alive():
                return
            self.timeout_thread = Thread(target=self.timeout_loop)
            self.timeout_thread.start()
        except Exception as e:
            raise e

    def reset_timeout(self):
        '''
        reset the election timeout after receiving heartbeat
        from the leader
        '''
        self.election_time = time.time() + config.random_timeout()

""" Main Raft Node class.
"""
import os

from .log_manager import *
from .election import *
from .transport import *
from .transport import *

class RaftNode:
    def __init__(self):
        # Initialize node name.
        if 'NAME' not in os.environ:
            raise 'env variable NAME missing!!'
        self.name = os.environ['NAME']

        if 'PEER_IPS' not in os.environ:
            raise 'env variable PEER_IPS missing!!'
        peers = os.environ['PEER_IPS'].split(",")

        # Initialize all fields.
        self.current_term = 0
        self.__log_manager = LogManager(self.name)
        self.__transport = Transport(peers, self.__log_manager)
        self.election = Election(self.current_term, self.__log_manager, transport=self.__transport)

        print(f"Raft Node up.\n Name: {self.name}\n current term: {self.current_term}\n"+
            f"Peers: {peers}")

    def serve_put_request(self, key, value):
        """
        Service the put request from client.

        Returns success ack, else errMsg.
        """
        if not self.election.is_node_leader():
            # TODO: Redirect to leader node.
            return False, "node not leader"
        
        log_item = self.__log_manager.get_log_entry(key, value)

        # Append log item to current node's log.
        index = self.__log_manager.append(log_item)
        self.__log_manager.output_log("Appended entry to leader's log.")

        # Push append entries to other peers.
        self.__transport.append_entry_to_peers(log_item, index)

        return True, ""
"""
This file contains the transport class that handles communication
between raft nodes.
"""
 
import grpc

from threading import Thread
from concurrent import futures

import raft_pb2
import raft_pb2_grpc
from .protocol_servicer import *

class Transport:
    def __init__(self, peers, log_manager):
        # log manager.
        self.log_manager = log_manager

        # list of grpc clients.
        self.peer_ips = peers

        # Initialize grpc client stubs to communicate with each client.
        self.peer_stubs = list()

        for ip in self.peer_ips:
            channel = grpc.insecure_channel(ip)
            self.peer_stubs.append(channel)

        # Start raft server.
        Thread(target=self.raft_server).start()

    def raft_server(self):
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    
        servicer = RaftProtocolServicer(self.log_manager)
        raft_pb2_grpc.add_RaftProtocolServicer_to_server(servicer, grpc_server)
        grpc_server.add_insecure_port('[::]:4000')
        
        print("Raft Node server listening on port:4000")
        grpc_server.start()
        grpc_server.wait_for_termination()
        print("Server terminated")

    def heartbeat(self, peer: str, message: dict = None) -> dict:
        '''
        If this node is the leader, it will send a heartbeat message
        to the follower at address `peer`
        :param peer: address of the follower in `ip:port` format
        :type peer: str
        :param message: heartbeat message; it consists current term and
                        address of this node (leader node)
        :type message: dict
        :returns: heartbeat message response as received from the follower
        :rtype: dict
        '''
        # TODO: Complete function
        channel = grpc.insecure_channel(peer)
        stub = raft_pb2_grpc.RaftProtocol(channel)
        request = raft_pb2.AERequest(is_heart_beat=True)
        # send the request
        response = stub.AppendEntries(request)

    def push_append_entry(self, peer_stub, entry, index):
        return 0

    def append_entry_to_peers(self, entry, index):
        '''
        This routine pushes the append entry to all peers.
        '''
        num_succ = 0
        for stub in self.peer_stubs:
            num_succ += self.push_append_entry(stub, entry, index)

        return num_succ

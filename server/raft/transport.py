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
            stub = raft_pb2_grpc.RaftProtocolStub(channel)

            self.peer_stubs.append(stub)

        # Start raft server.
        Thread(target=self.raft_server).start()

    ############### gRPC server ##########################

    def raft_server(self):
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    
        servicer = RaftProtocolServicer(self.log_manager)
        raft_pb2_grpc.add_RaftProtocolServicer_to_server(servicer, grpc_server)
        grpc_server.add_insecure_port('[::]:4000')
        
        print("Raft server listening on port:4000")
        grpc_server.start()
        grpc_server.wait_for_termination()
        print("Server terminated")

    ############### Election ##########################

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

    ############### AppendEntries RPC ##########################

    def push_append_entry(self, peer_stub, log_entry, index):
        prev_index = index - 1
        prev_log_entry = self.log_manager.get_log_at_index(prev_index)
        
        # Prepare appendRPC request.
        request = raft_pb2.AERequest(
            term = self.log_manager.get_current_term(),
            leader_id = self.log_manager.get_name(),
            start_index = index,
            prev_log_index = index - 1,
            prev_log_term = prev_log_entry.term,
            is_heart_beat=False,
        )
        # request.entries.extend([log_entry])

        resp = peer_stub.AppendEntries(request)
        print(f"Append entries resp {resp}")
        
        return 0

    def append_entry_to_peers(self, entry, index):
        '''
        This routine pushes the append entry to all peers.
        '''
        num_succ = 0
        for stub in self.peer_stubs:
            num_succ += self.push_append_entry(stub, entry, index)

        return num_succ

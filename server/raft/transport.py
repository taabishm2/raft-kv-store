""" 
This file contains the transport class that handles communication
between raft nodes.
"""
 
import grpc

from threading import Thread
from concurrent import futures

import raft_pb2
import raft_pb2_grpc
from protocol_servicer import *

class Transport:
    def __init__(self, peer_ips, log_manager):
        # log manager.
        self.log_manager = log_manager

        # list of grpc clients.
        self.peer_ips = peer_ips

        # Initialize grpc client stubs to communicate with each client.
        self.stubs = list()

        for ip in self.peer_ips:
            channel = grpc.insecure_channel(ip)
            self.stubs.append(channel)

        # Start raft server.
        Thread(target=self.raft_server).start()

    def raft_server(self):
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    
        servicer = RaftProtocolServicer("leader", self.log_manager)
        raft_pb2_grpc.add_RaftProtocolServicer_to_server(servicer, grpc_server)
        grpc_server.add_insecure_port('[::]:4000')
        
        print("Raft Node server listening on port:4000")
        grpc_server.start()
        grpc_server.wait_for_termination()
        print("Server terminated")
        


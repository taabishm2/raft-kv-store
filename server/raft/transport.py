"""
This file contains the transport class that handles communication
between logs nodes.
"""
 
from concurrent import futures
from threading import Thread

import grpc
import raft_pb2
import raft_pb2_grpc

from .log_manager import *
from .protocol_servicer import *


class Transport:
    def __init__(self, peers, log_manager):
        # TODO: passing log manager along seems off
        self.log_manager = log_manager

        # TODO: Isn't there a better way for this?
        assert 'PEER_IPS' in os.environ
        peers = os.environ['PEER_IPS'].split(",")
        self.peer_ips = peers

        # create a dictionary to store the peer IDs and their stubs
        peer_stubs = {}

        for ip in self.peer_ips:
            channel = grpc.insecure_channel(ip)
            stub = raft_pb2_grpc.RaftProtocolStub(channel)
            peer_stubs[ip] = stub

        # Start logs server.
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

    def send_heartbeat(self, peer)->raft_pb2.AEResponse():
        '''
        If this node is the leader, it will send a heartbeat message
        to the follower at address peer
        '''
        stub = self.peer_stubs[peer]
        request = raft_pb2.AERequest(
            term = self.log_manager.get_current_term(), is_heart_beat = True)
            # send the request
        response = stub.AppendEntries(request)
        print(f"Heartbeat response is {response}")
        return response

    ############### AppendEntries RPC ##########################

    def push_append_entry(self, peer_stub, index, entry: LogEntry):
        prev_index = index - 1
        prev_log_entry = self.log_manager.get_log_at_index(prev_index)
        
        # Prepare appendRPC request.
        request = raft_pb2.AERequest(
            term = self.log_manager.get_current_term(),
            leader_id = self.log_manager.get_name(),
            start_index = index,
            prev_log_index = prev_index,
            prev_log_term = prev_log_entry.term,
            is_heart_beat=False,
        )
        log_entry = raft_pb2.LogEntry(
            log_term = int(entry.term),
            command = raft_pb2.WriteCommand(
                key = entry.cmd_key,
                value = entry.cmd_val,
            )
        )
        request.entries.append(log_entry)

        resp = peer_stub.AppendEntries(request)
        print(f"Append entries resp {resp}")

        return 0

    def append_entry_to_peers(self, entry, index):
        '''
        This routine pushes the append entry to all peers.
        '''
        num_succ = 0
        for stub in self.peer_stubs.values():
            num_succ += self.push_append_entry(stub, index, entry)

        return num_succ

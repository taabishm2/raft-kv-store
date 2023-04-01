"""
This file contains the transport class that handles communication
between logs nodes.
"""

import os
from concurrent import futures

import grpc

import raft_pb2
import raft_pb2_grpc
from .utils import *
from .config import globals, NodeRole
from .log_manager import log_manager, LogEntry


class RaftProtocolServicer(raft_pb2_grpc.RaftProtocolServicer):

    def RequestVote(self, request, context):
        # TODO: Tabish
        return None

    def AppendEntries(self, request, context):
        if request.is_heart_beat:
            return self.heartbeat_handler(request=request)

        start_index, prev_term = request.start_index, request.prev_log_term
        log_entries = [from_grpc_log_entry(entry) for entry in request.entries]

        # Try to append entry at given index.
        if not log_manager.overwrite(start_index, log_entries, prev_term):
            return raft_pb2.AEResponse(is_success=False, error="Append Entries overwrite failed")

        return raft_pb2.AEResponse(is_success=True)

    def heartbeat_handler(self, request) -> tuple:
        """
        Handler function for FOLLOWER node to validate heartbeat RECEIVED from leader

        :param request: AERequest req for heartbeat data sent by the leader node
        :returns: term and latest commit_id of this (follower) node
        :rtype: tuple
        """
        try:
            term = request.term
            if globals.current_term <= term:
                # Got heartbeat from a leader with valid term
                log_manager.reset_timeout()
                print(f'got heartbeat from leader {log_manager.leader_ip}')
                log_manager.role = NodeRole.Follower

                # Update my term to leader's term
                log_manager.current_term = max(term, log_manager.current_term)

            return raft_pb2.AEResponse(log_manager.current_term, is_success=True)
            # TODO: NNED TO SEND LATEST COMMIT ID ALSOOO???
        except Exception as e:
            raise e


class Transport:
    def __init__(self):
        self.peer_ips = os.environ['PEER_IPS'].split(",")
        # create a dictionary to store the peer IPs and their stubs
        self.peer_stubs = {ip: raft_pb2_grpc.RaftProtocolStub(grpc.insecure_channel(ip)) for ip in self.peer_ips}

    def send_heartbeat(self, peer) -> raft_pb2.AEResponse():
        """ If this node is leader, send heartbeat to the follower at address `peer`"""
        request = raft_pb2.AERequest(term=globals.current_term, is_heart_beat=True)
        # send the request
        response = self.peer_stubs[peer].AppendEntries(request)
        print(f"Heartbeat response is {response}")
        return response

    # AppendEntries RPC

    def append_entry_to_peers(self, entry, index):
        success_count = 0
        for stub in self.peer_stubs.values():
            success_count += self.push_append_entry(stub, index, entry)

        return success_count

    def push_append_entry(self, peer_stub, index, entry: LogEntry):
        prev_index = index - 1
        prev_log_entry = log_manager.get_log_at_index(prev_index)

        # Prepare appendRPC request.
        request = raft_pb2.AERequest(
            term=globals.current_term,
            leader_id=globals.name,
            start_index=index,
            prev_log_index=prev_index,
            prev_log_term=prev_log_entry.term,
            is_heart_beat=False,
        )

        log_entry = raft_pb2.LogEntry(
            log_term=int(entry.term),
            command=raft_pb2.WriteCommand(
                key=entry.cmd_key,
                value=entry.cmd_val,
            )
        )
        request.entries.append(log_entry)

        resp = peer_stub.AppendEntries(request)
        print(f"Append entries resp {resp}")

        return 0

    def request_vote(self, peer):
        request = raft_pb2.VoteRequest(term=globals.current_term, candidate_id=globals.name,
                                       last_log_index=log_manager.get_last_index(),
                                       last_log_term=log_manager.get_latest_term())
        response = self.peer_stubs[peer].RequestVote(request)
        print(f"VoteRequest response is {response}")
        return response


def from_grpc_log_entry(entry):
    write_command = entry.command
    return LogEntry(entry.log_term, write_command.key, write_command.value)


def main():
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))

    servicer = RaftProtocolServicer()
    raft_pb2_grpc.add_RaftProtocolServicer_to_server(servicer, grpc_server)
    grpc_server.add_insecure_port('[::]:4000')

    log_me(f"{globals.name} GRPC server listening on: 4000")
    grpc_server.start()
    grpc_server.wait_for_termination()
    log_me(f"{globals.name} GRPC server terminated")


transport = Transport()

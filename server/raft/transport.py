"""
This file contains the transport class that handles communication
between logs nodes.
"""

import os
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep, time

import grpc
import raft_pb2
import raft_pb2_grpc

from .config import NodeRole, globals
from .log_manager import LogEntry, log_manager
from .utils import *

##################### Helper Utils ##################################

def to_grpc_log_entry(entry : LogEntry):
    log_entry = raft_pb2.LogEntry(
            log_term = int(entry.term),
            command = raft_pb2.WriteCommand(
                key = entry.cmd_key,
                value = entry.cmd_val,
            )
        )
    
    return log_entry

def from_grpc_log_entry(entry):
    write_command = entry.command
    log_entry = LogEntry(entry.log_term,
        write_command.key, write_command.value)
    
    return log_entry

#####################################################################

class RaftProtocolServicer(raft_pb2_grpc.RaftProtocolServicer):

    def RequestVote(self, request, context):
        log_me(f"Vote Requested by {request.candidate_id}")
        # Vote denied if my term > candidate's or (terms equal but (my log is longer / I have already voted)
        if globals.current_term > request.last_log_term or (
                globals.current_term == request.last_log_term and (
                globals.voted_for is not None or log_manager.get_last_index() > request.last_log_index)):
            return raft_pb2.VoteResponse(term=globals.current_term, vote_granted=False)

        # If a candidate/leader discovers its term is out of date, immediately revert to follower
        globals.current_term = request.last_log_term
        # TODO: what else to do at this transition?
        globals.state = NodeRole.Follower
        globals.voted_for = request.candidate_id

        return raft_pb2.VoteResponse(term=request.last_log_term, vote_granted=True)

    def AppendEntries(self, request, context):
        if globals.is_unresponsive:
            log_me("Am going to sleepzzzz")
            while True:
                sleep(1)

        #TODO: if RPC term is valid, update globals.leader_name and globals.term (in case leadership changed)
        if request.is_heart_beat:
            return self.heartbeat_handler(request=request)

        start_index, prev_term = request.start_index, request.prev_log_term
        log_entries = [from_grpc_log_entry(entry) for entry in request.entries]

        # Try to append entry at given index.
        if not log_manager.overwrite(start_index, log_entries, prev_term):
            log_me(f"AppendEntries request from {request.leader_id} failed")
            return raft_pb2.AEResponse(is_success=False, error="Append Entries overwrite failed")
        
        # AppendEntries RPC success.
        log_me(f"AppendEntries request success from {request.leader_id}, starting with {request.start_index}")
        return raft_pb2.AEResponse(is_success=True)

    def heartbeat_handler(self, request):
        """
        Handler function for FOLLOWER node to validate heartbeat RECEIVED from leader

        :param request: AERequest req for heartbeat data sent by the leader node
        :returns: term and latest commit_id of this (follower) node
        """
        # TODO: In case this node is a candidate or leader,
        # 1. it should become a follower once again.
        # 2. stop it's previous role duties.
        try:
            term = request.term
            if globals.current_term <= term:
                # Got heartbeat from a leader with valid term
                rand_timeout = random_timeout(globals.LOW_TIMEOUT, globals.HIGH_TIMEOUT)
                globals.curr_rand_election_timeout = time() + rand_timeout
                # Set new leader's name.
                globals.set_leader_name(request.leader_id)

                log_me(f'Received heartbeat from leader {globals.leader_name}')
                globals.role = NodeRole.Follower

                # Update my term to leader's term
                globals.current_term = max(term, globals.current_term)

            return raft_pb2.AEResponse(term=globals.current_term, is_success=True)
            # TODO: NNED TO SEND LATEST COMMIT ID ALSOOO???
        except Exception as e:
            raise e


class Transport:
    def __init__(self):
        self.peer_ips = os.environ['PEER_IPS'].split(",")
        # create a dictionary to store the peer IPs and their stubs
        self.peer_stubs = {ip: raft_pb2_grpc.RaftProtocolStub(grpc.insecure_channel(ip)) for ip in self.peer_ips}

    def send_heartbeat(self, peer):
        """ If this node is leader, send heartbeat to the follower at address `peer`"""
        peer_stub = self.peer_stubs[peer]
        last_idx = log_manager.get_last_index()
        if last_idx == 0:
            request = raft_pb2.AERequest(
                leader_id=globals.name,
                term=globals.current_term,
                is_heart_beat=True)
            response = self.peer_stubs[peer].AppendEntries(request)
        else:
            success, response = self.push_append_entry(
                peer_stub, last_idx, [log_manager.get_log_at_index(last_idx)], True)

        return response

    # AppendEntries RPC
    def append_entry_to_peers(self, entry, index):
        success_count = 0
        # Use thread pool to submit rpcs to peers.
        num_peers = len(self.peer_stubs)
        with ThreadPoolExecutor(max_workers=num_peers) as executor:
            future_rpcs = {executor.submit(self.push_append_entry, stub, index, [entry])
                for stub in self.peer_stubs.values()}
            for completed_task in as_completed(future_rpcs):
                try:
                    is_complete, _ = completed_task.result()
                    success_count += is_complete
                except Exception as exc:
                    # Unresponsive clients, Internal errors...
                    log_me(f'generated an exception: {exc}')

        log_me(f"AppendEntries RPC success from {success_count} replicas")

        # Return whether append entries is successful on a majority of peers (
        # excluding the leader node).
        return (success_count  >= (num_peers) // 2)

    def push_append_entry(self, peer_stub, index, entries: list[LogEntry], is_heartbeat = False):
        # Trivial failure case.
        if index < 0:
            return 0, None

        prev_index = index - 1
        prev_log_entry = log_manager.get_log_at_index(prev_index)

        # Prepare appendRPC request.
        request = raft_pb2.AERequest(
            term=globals.current_term,
            leader_id=globals.name,
            start_index=index,
            prev_log_index=prev_index,
            prev_log_term=prev_log_entry.term,
            is_heart_beat=is_heartbeat,
            commit_index=globals.commitIndex
        )

        for entry in entries:
            log_entry_grpc = to_grpc_log_entry(entry)
            request.entries.append(log_entry_grpc)

        # Call appendEntries RPC with 5 second timeout.
        resp = peer_stub.AppendEntries(request, timeout=5)
        if not resp.is_success:
            entries[1:] = entries
            entries[0] = prev_log_entry
            # Retry with updated entries list.
            return self.push_append_entry(peer_stub, index - 1, entries)

        return 1, resp

    def request_vote(self, peer):
        request = raft_pb2.VoteRequest(term=globals.current_term, candidate_id=globals.name,
                                       last_log_index=log_manager.get_last_index(),
                                       last_log_term=log_manager.get_latest_term())
        response = self.peer_stubs[peer].RequestVote(request)
        log_me(f"VoteRequest response from {peer} is {response.vote_granted}")
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

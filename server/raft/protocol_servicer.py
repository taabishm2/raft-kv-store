'''
This file services raft protocol rpcs.
'''
import raft_pb2
import raft_pb2_grpc

from .log_manager import *


class RaftProtocolServicer(raft_pb2_grpc.RaftProtocolServicer):
    def __init__(self, log_manager: LogManager):
        super().__init__()

        self.__log_manager = log_manager

    def RequestVote(self, request, context):
        """
        RequestVote for raft leader node election.
        """
        return None

    def AppendEntries(self, request, context):
        """
        AppendEntries for leader raft node to override/append its
        log entries into followers.
        """
        self.__log_manager.output_log(f'Got AppendEntries request {request}')

        # Got request params.
        start_index = request.start_index
        log_entries = []
        prev_term = request.prev_log_term

        for entry in request.entries:
            log_entries.append(from_grpc_log_entry(entry))

        # Try to append entry at given index.
        if not self.__log_manager.overwrite(start_index, log_entries, prev_term):
            return raft_pb2.AEResponse(is_success=False,
             error="Append Entries overwrite failed")            

        return raft_pb2.AEResponse(is_success=True)

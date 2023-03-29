'''
This file services raft protocol rpcs.
'''
import raft_pb2
import raft_pb2_grpc

class RaftProtocolServicer(raft_pb2_grpc.RaftProtocolServicer):
    def __init__(self, log_manager):
        super().__init__()

        self.log_manager = log_manager

    def RequestVote(self, request, context):
        """RequestVote for raft leader node election.
        """
        return None

    def AppendEntries(self, request, context):
        """AppendEntries for leader raft node to override/append its log entries into followers.
        """
        print(f'Got AppendEntries request {request.start_index} {request.term} {request.prev_log_index} {request.prev_log_term}')

        return raft_pb2.AEResponse()
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
        print(f'Got AppendEntries request {request.start_index}, {request.term}')

        """
        Check if previous entry matches, and overwrite. Else, return failure.
        """

        return None
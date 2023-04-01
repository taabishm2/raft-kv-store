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
        if request.is_heart_beat:
            self.heartbeat_handler(request=request)
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
    
    def heartbeat_handler(self, request) -> tuple:
        '''
        Handler function for the FOLLOWER node to validate the heartbeat RECIEVED
        from the leader

        :param request: AERequest req for heartbeat data sent by the leader node
        :returns: term and latest commit_id of this (follower) node
        :rtype: tuple
        '''
        try:
            term = request.term
            if self.__log_manager.get_current_term() <= term:
                # Got heartbeat from a leader with valid term
                self.__log_manager.reset_timeout()
                print(f'got heartbeat from leader {self.__log_manager.leader_ip}')
                if self.__log_manager.role == NodeRole.Candidate:
                    self.__log_manager.role = NodeRole.Follower
                elif self.__log_manager.role == NodeRole.Leader:
                    self.__log_manager.role = NodeRole.Follower

                # Update my term to leader's term
                if self.__log_manager.current_term < term:
                    self.__log_manager.current_term = term

            return raft_pb2.AEResponse(self.__log_manager.current_term, is_success=True)
            # TODO: NNED TO SEND LATEST COMMIT ID ALSOOO???
        except Exception as e:
            raise e

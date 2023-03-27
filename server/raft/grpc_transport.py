## This file contains all the grpc-related classes.
import raft_pb2
import raft_pb2_grpc

class Transport:
    def __init__(self):
        # list of grpc clients.
        self.stubs = None

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
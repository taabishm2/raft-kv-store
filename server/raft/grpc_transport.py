## This file contains all the grpc-related classes.
import raft_pb2
import raft_pb2_grpc

class Transport:
    def __init__(self):
        # list of grpc clients.
        self.stubs = None

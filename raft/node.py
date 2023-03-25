
import enum
from log import *

# Using enum class create enumerations
class NodeRole(enum.Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

class RaftNode:
    def __init__(self, name):
        self.name = name

        self.current_term = 0
        self.role = NodeRole.Follower

        self.log_manager = LogManager()

    def get 

    

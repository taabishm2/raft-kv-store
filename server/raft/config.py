from random import randrange
from os import getenv

LEADER = 0
CANDIDATE = 1
FOLLOWER = 2
# Syntax: os.getenv(key, default).
# Heartbeat timeout T= 250ms. Random timeout in range [T, 2T] unless
# specified in the env vars
LOW_TIMEOUT = int(getenv('LOW_TIMEOUT', 250))
HIGH_TIMEOUT =  int(getenv('HIGH_TIMEOUT', 500))

# REQUESTS_TIMEOUT = 50
# Heartbeat is sent every 100ms
HB_TIME = int(getenv('HB_TIME', 100))
# MAX_LOG_WAIT = int(getenv('MAX_LOG_WAIT', 150))

def random_timeout():
    '''
    return random timeout number
    '''
    return randrange(LOW_TIMEOUT, HIGH_TIMEOUT) / 1000

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))
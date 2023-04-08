import client
from time import *

def test1():
    # Leader in minority case.
    #
    # Remove leader node. Observe that PUT fails because of no-consensus.
    # After adding the same node back, it should become a follower.
    #
    client.send_put("Key1", "Val1")
    client.send_put("Key1", "Val1")
    client.send_mult_get(["Key1"])

    print("\n\n\n")

if __name__ == '__main__':
    test1()
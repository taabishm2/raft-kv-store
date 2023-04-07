import client
from time import *

def test1():
    # Leader in minority case.
    # 
    # Remove leader node. Observe that PUT fails because of no-consensus.
    # After adding the same node back, it should become a follower.
    # 
    client.send_put("Key1", "Val1")
    client.send_get("Key1")

    partitioned_leader = client.LEADER_NAME
    client.send_remove_node(partitioned_leader)

    print(f"I removed {client.LEADER_NAME} from cluster")

    print("\n\n\n")
    sleep(20)

    client.best_effort_put("Key2", "Val2")

    print(f"I think, the current leader is {client.LEADER_NAME}")

    print("\n\n\n")
    sleep(20)

    client.send_add_node(partitioned_leader)

    print("\n\n\n")
    sleep(20)

    print(f"I think, the current leader is {client.LEADER_NAME}")

    client.send_get("Key2")
    
    print("\n\n\n")

def test2():
    # Leader in majority.
    # 
    # Remove a follower node. Observe that PUT, GET succeeds.
    # Follower should keep triggering election with no vain.
    # After adding the same node back, it should become the leader.
    # 
    client.send_put("Key1", "Val1")
    client.send_get("Key1")

    print(f"I think, the current leader is {client.LEADER_NAME}")
    follower = client.get_follower()
    print(f"Removing follower {follower}")

    client.send_remove_node(follower)

    print("\n\n\n")
    sleep(20)

    # should succeed.
    client.best_effort_put("Key2", "Val2")
    print(f"I think, the current leader is {client.LEADER_NAME}")

    print("\n\n\n")
    sleep(20)

    # Add the node back.
    # The node should become the leader.
    client.send_add_node(follower)

    print("\n\n\n")
    sleep(20)

    client.send_get("Key2")
    print(f"I think, the current leader is {client.LEADER_NAME}")
    print("\n\n\n")


if __name__ == '__main__':
    test1()
    test2()
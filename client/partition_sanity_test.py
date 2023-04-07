import client
from time import *

def test1():
    # Leader partition in majority.
    client.send_put("Key1", "Val1")
    client.send_get("Key1")

    print(f"Leader in my opinion is {client.LEADER_NAME} {client.NODE_IPS[client.LEADER_NAME]}")

    # Partition non-leader node.
    client.send_remove_node(client.LEADER_NAME)

    sleep(20)

    client.best_effort_put("Key2", "Val2")

    sleep(20)

    client.send_add_node(client.NODE_DOCKER_IPS[client.LEADER_NAME])

    sleep(20)

    client.send_get("Key2")


if __name__ == '__main__':
    test1()
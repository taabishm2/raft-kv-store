import client
from time import *

def test1():
    # Testing multi-get.
    # client.send_put("Key1", "Val1")
    # client.send_put("Key2", "Val2")
    # resp = client.send_mult_get(["Key1", "Key2"])

    # # Testing multi-put.
    client.send_multi_put(["Key3",  "Key4"], ["Val3", "Val4"])
    # resp = client.send_mult_get(["Key3", "Key4"])
    client.send_get("Key3")

    # for v in resp.get_vector:
    #     print(f"{v.key} {v.value}")

if __name__ == '__main__':
    test1()
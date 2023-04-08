import client1
from time import *

def read_after_write_consistency():
    #
    # Reads should observe the previous writes
    # 
    for i in range(10):
        resp = client1.send_put(f"Key{i}", f"Val{i}")
        assert resp.error is "", f":( putting Key{i} resulted in error"
        
    for i in range(10):  
        resp = client1.send_get(f"Key{i}")
        assert resp.key_exists is True, f":( Written Key{i} not found in db"
        assert resp.value == f"Val{i}", f":( Value read is not same as value written for Key{i}. Value read = {resp.value}"

    print(f":) Read after write consistency passed!!\n Values read are consistent with values written.\n\n")
def read_after_read_consistency():
    # 
    # two consecutive Reads should return the same value
    # 
    client1.send_put("Key123", "Val123")
    resp1 = client1.send_get("Key123")
    assert resp1.key_exists is True, "Written Key{i} not found in db"

    client1.send_put("Key1234", "Val1234")
    resp2 = client1.send_get("Key123")
    assert resp2.key_exists is True, "Written Key{i} not found in db"

    assert resp1.value == resp2.value, ":( Values mismatch"

    print(f":) Read after read consistency passed!!\n\n")

def durability_test():
    # 
    # Commit is persisted across leader crashes
    # 
    client1.send_put("KeyDurable", "ValDurable")
    resp = client1.send_get("KeyDurable")
    assert resp.key_exists is True, "Written Key{i} not found in db"

    print(f"Removing the leader node {client1.LEADER_NAME} from cluster")
    client1.send_remove_node(client1.LEADER_NAME)
    
    resp = client1.send_get("KeyDurable")
    assert resp.key_exists is True, "Key is not persisted across crash."
    print(f":) Durabilty test passed!!\n\n")

if __name__ == '__main__':
    print("Running Read after write consistency test!!\n\n")
    read_after_write_consistency()
    print("======================================================")
    print("\n\nRunning Read after read consistency test!!\n\n")
    read_after_read_consistency()
    print("======================================================")
    print("\n\nRunning durability test!!\n\n")
    durability_test()
    print("======================================================")
cd /raft-kv-store/
python3 -u -m server.server > /raft-kv-store/raft/logcache/server.out &
tail -f /dev/null